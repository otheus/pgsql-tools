#!/usr/bin/perl
use strict;

use File::Spec;
use Data::Dumper;
use POSIX qw(strftime);
use Fatal qw(open);

#my ( $OPTIONAL_HEADER, @HEADER_ELEMENTS ) = get_optional_header_re( '%m %u@%d %p %r ' );
my ( $OPTIONAL_HEADER, @HEADER_ELEMENTS ) = get_optional_header_re( '%t (%r) [%p]: [%l-1] user=%u,db=%d ' );
my $ANALYZE_DB  = shift || '*';
my $HTML_OUTPUT = 0;
my $SORT_ORDER  = 'sum';          # possible: min, max, sum, count, avg

my $globals = {};
my $queries = {};

debug( 'Starting' );

normalize_data();

debug( 'Stage1 complete' );

sort_data();

debug( 'Stage2 complete' );

write_raport();

exit;

sub get_optional_header_re {
    my $prefix = shift;

    my %re = (
        'u' => '[a-z0-9_]*',
        'd' => '[a-z0-9_]*',
        'r' => '(?:\d{1,3}(?:\.\d{1,3}){3}\(\d+\)|\[local\])?',
        'h' => '\d{1,3}(?:\.\d{1,3}){3}|\[local\]',
        'p' => '\d+',
        't' => '\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d (?:[A-Z]+|\+\d\d\d\d)',
        'm' => '\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+ (?:[A-Z]+|\+\d\d\d\d)',
        'l' => '\d+',
        'i' => '(?:BEGIN|COMMIT|DELETE|INSERT|ROLLBACK|SELECT|SET|SHOW|UPDATE)',
        'x' => '\d+',
        'c' => '[a-f0-9]+\.[a-f0-9]+',
    );

    my @known_keys = keys %re;
    my $known_re = join '|', @known_keys;

    my @matched = ();

    $prefix =~ s/([()\[\]])/\\$1/g;
    $prefix =~ s/%($known_re)/push @matched, $1;'('.$re{$1}.')'/ge;
    return $prefix, @matched;
}

sub write_raport {
    print "<style>td {border: solid 1pt;}</style>\n" if $HTML_OUTPUT;
    print_raport_summary();
    print "<table>\n" if $HTML_OUTPUT;
    print_raport_header();
    for my $normal ( @{ $globals->{ 'queries_list' } } ) {
        my $d = delete $queries->{ $normal };
        $d->{ 'normal' } = $normal;
        raport_data( $d );
    }
    print "</table>\n" if $HTML_OUTPUT;
    return;
}

sub raport_columns {
    if ( $HTML_OUTPUT ) {
        print "<tr><td>" . join( "</td><td>", @_ ) . "</td></tr>\n";
    }
    else {
        print join( "\t", @_ ) . "\n";
    }
    return;
}

sub print_raport_summary {
    print "<table>\n" if $HTML_OUTPUT;
    raport_columns( "Total-time:",  sprintf "%d", $globals->{ 'time' } );
    raport_columns( "Total-count:", sprintf "%d", $globals->{ 'count' } );
    raport_columns( "First-header:", $globals->{ 'first_header' } ) if $globals->{ 'first_header' };
    raport_columns( "Last-header:",  $globals->{ 'last_header' } )  if $globals->{ 'last_header' };
    print "</table>\n" if $HTML_OUTPUT;
    return;
}

sub print_raport_header {
    raport_columns( qw( query count min avg max totaltime count% time% factor-x fastest slowest ) );
    return;
}

sub raport_data {
    my $d = shift;
    my @c = ();
    push @c, $d->{ 'normal' };
    push @c, $d->{ 'count' };
    push @c, sprintf "%.2f", $d->{ 'min' };
    push @c, sprintf "%.2f", $d->{ 'avg' };
    push @c, sprintf "%.2f", $d->{ 'max' };
    push @c, sprintf "%.2f", $d->{ 'sum' };
    push @c, sprintf "%.2f", 100 * $d->{ 'count' } / $globals->{ 'count' };                                             # count %
    push @c, sprintf "%.2f", 100 * $d->{ 'sum' } / $globals->{ 'time' };                                                # time %
    push @c, sprintf "%.2f", ( $d->{ 'sum' } / $globals->{ 'time' } ) / ( $d->{ 'count' } / $globals->{ 'count' } );    # factor x
    push @c, $d->{ 'min_sql' };
    push @c, $d->{ 'max_sql' };
    raport_columns( @c );
    return;
}

sub sort_data {

    $globals->{ 'queries_list' } = [
        sort { $queries->{ $b }->{ $SORT_ORDER } <=> $queries->{ $a }->{ $SORT_ORDER } }
            keys %{ $queries }
    ];
    return;
}

sub normalize_data {
    my $last = {};
    my $read = 0;

    while ( my $line = <STDIN> ) {
        chomp $line;
        $read++;
        print STDERR '#' if 0 == $read % 1000;

        my %prefix;
        if ( $line =~ s/^($OPTIONAL_HEADER)//oi ) {
            my $header = $1;
            @prefix{ @HEADER_ELEMENTS } = $header =~ /$OPTIONAL_HEADER/oi;
            $globals->{ 'first_header' } = $header unless $globals->{ 'first_header' };
            $globals->{ 'last_header' } = $header;
        }

        if ( $line =~ m{ \A \s*  LOG: \s+ duration: \s+ (\d+\.\d+) \s+ ms \s+ (?: statement | execute[^:]* ): \s+ (.*?) \s* \z }xms ) {

            my ( $time, $sql ) = ( $1, $2 );
            store_normalized_data( $last ) if $last->{ 'time' };
            $last = {
                'time'   => $time,
                'sql'    => $sql,
                'prefix' => \%prefix,
            };

        }
        elsif ( $line =~ m{ \A \s* (?: LOG | NOTICE | HINT | DETAIL | WARNING | PANIC | ERROR | FATAL | CONTEXT ) : \s{1,2} }xms ) {
            store_normalized_data( $last ) if $last->{ 'time' };
            $last = {};
            next;
        }
        else {
            next unless $last->{ 'time' };
            $last->{ 'sql' } .= ' ' . $line;
        }
    }
    store_normalized_data( $last ) if $last->{ 'sql' };
    return;
}

sub store_normalized_data {
    my $d = shift;
    if (   ( $ANALYZE_DB )
        && ( $ANALYZE_DB ne '*' )
        && ( $d->{ 'prefix' }->{ 'd' } ne $ANALYZE_DB ) )
    {
        return;
    }
    my $T = $d->{ 'time' };

    $globals->{ 'count' }++;
    $globals->{ 'time' } += $T;

    my $sql = $d->{ 'sql' };
    $sql =~ s/^\s*//;
    $sql =~ s/\s*$//;
    $sql =~ s/\s+/ /g;

    my $std = lc $sql;
    $std =~ s/'[^']*'/?/g;
    $std =~ s/\bnull\b/?/g;
    $std =~ s/\s* ( <> | >= | <= | <> | \+ | - | > | < | = ) \s* (\d+)(and|or) \s+ / $1 $2 $3 /x;
    $std =~ s/\b\d+\b/?/g;
    $std =~ s/ in \(\s*\$?\?(?:\s*,\s*\$?\?)*\s*\)/IN (?,,?)/gi;
    $std =~ s/ \s* ( \bI?LIKE\b | <> | >= | <= | <> | \+ | - | > | < | = | ~ ) \s* / \U$1\E /gix;
    $std =~ s/\s*;\s*$//;
    $std =~ s/\s+ = \s+ - \s+ \? \s*/ = ? /gx;
    if ( $std =~ s{ \A declare \s+ (\S+) \s+ (.*?) \s+ for \s+ (.*) \z }{declare ? $2 for $3}xms ) {
        $globals->{ 'cursor' }->{ $1 } = $3;
    }
    $std =~ s{ \A (fetch \s+ .*? \s+ from ) \s+ (\S+) \s* \z}{$1 [$globals->{'cursor'}->{$2}]}xms;
    $std =~ s{ \A prepare\s+ [^\s(]+ }{prepare STATEMENT}xms;
    $std =~ s{ \A deallocate \s+ \S+ }{deallocate STATEMENT}xms;

    $std =~ s{ \A (?: abort | rollback | rollback \s+ transaction ) \s* \z }{rollback}xms;
    $std =~ s{ \A (?: end | commit | commit \s+ transaction ) \s* \z }{commit}xms;
    $std =~ s{ \A (?: begin | begin \s+ work | begin \s+ transaction | start \s+ transaction ) \s* \z }{begin}xms;

    $std =~ s/--.*$//mg;
    $std =~ s{/\*.*?\*/}{}sg;
    $std =~ s/^\s*//;
    $std =~ s/\s*$//;
    $std =~ s/\s+/ /g;
    $std =~ s/\?+/?/g;

    my $ID    = qr{
        (?:
            [a-z_0-9]+
            |
            " (?: [^"]* | "" )+ "
        )
    }ix;
    my $FIELD = qr{
        (?:
            (?:
                $ID(?:\.$ID)?
            )
            (?:
                \s*
                [<>=]+
                \s*
                (?: $ID | \? )
            )?
            (?:
                \s+
                as
                \s+
                $ID
            )?
        )
    }ixo;
    $std =~ s/select $FIELD(?:\s*,\s*$FIELD)* FROM/select SOME_FIELDS from/goi;

    $queries->{ $std } ||= {};
    my $Q = $queries->{ $std };

    $Q->{ 'count' }++;
    $Q->{ 'sum' } += $T;
    $Q->{ 'avg' } = $Q->{ 'sum' } / $Q->{ 'count' };
    @$Q{ qw(min min_sql) } = ( $T, $sql ) if !$Q->{ 'min' } || $Q->{ 'min' } > $T;
    @$Q{ qw(max max_sql) } = ( $T, $sql ) if !$Q->{ 'max' } || $Q->{ 'max' } < $T;

    return;
}

sub debug {
    my $what = shift;
    printf STDERR '[%s] %s%s', strftime( '%Y-%m-%d %H:%M:%S', localtime time ), $what, "\n";
    return;
}
