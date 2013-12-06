#!/usr/bin/perl
use strict;

use File::Spec;
use Data::Dumper;
use POSIX qw(strftime);
use Fatal qw(open);
use bignum;
use HTTP::Date;

my $MINIMAL_IDLE_TO_REPORT = 100;
my ( $OPTIONAL_HEADER, @HEADER_ELEMENTS ) = get_optional_header_re( '%m %u@%d %p %r ' );
my $ANALYZE_DB = shift || '*';
my $COMMIT_SQL = qr{(?:COMMIT|ROLLBACK|END|ABORT)}i;
my $BEGIN_SQL  = qr{(?:BEGIN|START)}i;

my %pids = ();
parse_stdin();

exit;

sub get_optional_header_re {
    my $prefix = shift;

    my %re = (
        'u' => '[a-z0-9_]+',
        'd' => '[a-z0-9_]+',
        'r' => '\d{1,3}(?:\.\d{1,3}){3}\(\d+\)|\[local\]',
        'h' => '\d{1,3}(?:\.\d{1,3}){3}|\[local\]',
        'p' => '\d+',
        't' => '\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d (?:[A-Z]+|\+\d\d\d\d)',
        'm' => '\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+ (?:[A-Z]+|\+\d\d\d\d)',
        'l' => '[A-Z]+',
        'i' => '(?:BEGIN|COMMIT|DELETE|INSERT|ROLLBACK|SELECT|SET|SHOW|UPDATE)',
    );

    my @known_keys = keys %re;
    my $known_re = join '|', @known_keys;

    my @matched = ();

    $prefix =~ s/([()\[\]])/\\$1/g;
    $prefix =~ s/%($known_re)/push @matched, $1;'('.$re{$1}.')'/ge;
    return $prefix, @matched;
}

sub parse_stdin {
    my $last = {};
    my $read = 0;

    while ( my $line = <STDIN> ) {
        chomp $line;
        $read++;
        print STDERR '#' if 0 == $read % 1000;

        my @temp = $line =~ m/^$OPTIONAL_HEADER/i;
        $line =~ s/^$OPTIONAL_HEADER//i;
        my %prefix;
        @prefix{ @HEADER_ELEMENTS } = @temp;

        if ( $line =~ m{ \A \s*  LOG: \s+ duration: \s+ (\d+\.\d+) \s+ ms \s+ (?: statement | execute[^:]* ): \s+ (.*?) \s* \z }xms ) {

            my ( $time, $sql ) = ( $1, $2 );
            process_query( $last ) if $last->{ 'time' };
            $last = {
                'time'   => $time,
                'sql'    => $sql,
                'prefix' => \%prefix,
            };

        }
        elsif ( $line =~ m{ \A \s* (?: LOG | NOTICE | HINT | DETAIL | WARNING | PANIC | ERROR ) : \s{1,2} }xms ) {
            process_query( $last ) if $last->{ 'time' };
            $last = {};
            next;
        }
        else {
            next unless $last->{ 'time' };
            $last->{ 'sql' } .= ' ' . $line;
        }
    }
    process_query( $last ) if $last->{ 'sql' };
    return;
}

sub process_query {
    my $d = shift;
    if (   ( $ANALYZE_DB )
        && ( $ANALYZE_DB ne '*' )
        && ( $d->{ 'prefix' }->{ 'd' } ne $ANALYZE_DB ) )
    {
        return;
    }
    return unless $d->{ 'prefix' }->{ 'm' };
    return unless $d->{ 'prefix' }->{ 'p' };

    my $end_time = get_ms_from_timestamp( $d->{ 'prefix' }->{ 'm' } );
    return unless $end_time;
    my $beginning_time = $end_time - $d->{ 'time' };

    my $pid = $d->{ 'prefix' }->{ 'p' };

    if ( $pids{ $pid } ) {
        my $previous_end = $pids{ $pid }->{ 'last' };
        if ( $beginning_time - $previous_end >= $MINIMAL_IDLE_TO_REPORT ) {
            printf( "%ums @ %s (pid: %s), queries:\n", $beginning_time - $previous_end, $d->{ 'prefix' }->{ 'm' }, $pid );
            print "  - $_\n" for ( @{ $pids{ $pid }->{ 'queries' } }, $d->{ 'sql' } );
            print "\n";
        }
    }
    if ( $d->{ 'sql' } =~ m{\A\s*$COMMIT_SQL\s*;?\s*\z} ) {
        delete $pids{ $pid };
        return;
    }
    my $qr = qr{\A\s*(?:$COMMIT_SQL\s*;\s*)?$BEGIN_SQL\s*;?\s*\z};
    if ( $d->{ 'sql' } =~ m{\A\s*(?:$COMMIT_SQL\s*;\s*)?$BEGIN_SQL\s*;?\s*\z} ) {
        $pids{ $pid } = {
            'last'    => $end_time,
            'queries' => [ $d->{ 'sql' } ],
        };
        return;
    }
    if ( $pids{ $pid } ) {
        $pids{ $pid }->{ 'last' } = $end_time;
        push @{ $pids{ $pid }->{ 'queries' } }, $d->{ 'sql' };
    }
    return;
}

sub get_ms_from_timestamp {
    my $timestamp = shift;
    return unless $timestamp =~ m{\A(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d{1,3})?)};
    return 1000 * str2time( $1 );
}
