#!/usr/bin/env perl
use strict;
use DBI;
use Data::Dumper;

my @db_conn_data = @ARGV;

our @USE_SCHEMAS = qw( public );
my %cant_be_together = (
    'public.posts' => 'public.topics',
    'public.topics' => 'public.posts',
    'public.forums' => 'public.posts',
    'public.galleries' => 'public.photos',
    'public.photos' => 'public.galleries',
);
my %tablespaces = ();
my $indexes_for = {};
my $size_of = {};

TRY:
while (1) {
    my $data = get_data_from_db();
    %tablespaces = ();
    printf "%-6s  %-42s ||| %-42s ||| %s\n", '', "tablespace: primary", "tablespace: secondary", "score";
    printf "%-6s %s+++%s+++%s\n", '', ("-"x(42+2))x2, "-"x10;
    printf '%-6s  %6s | %-15s | %-15s ||| %6s | %-15s | %-15s ||| score' . "\n", '', 'tables', 'idx_tup_fetch', 'writes', 'tables', 'idx_tup_fetch', 'writes', ;
    printf '%-6s %8s+%17s+%17s+++%8s+%17s+%17s+++%s' . "\n", '', map { "-"x$_ } qw(8 17 17 8 17 17 10) ;

    for my $table_name (keys %{ $data }) {
        my $table = $data->{ $table_name };
        my @possible_tablespaces = get_possible_tablespaces($table_name);
        my $tbspc_name = $possible_tablespaces[rand @possible_tablespaces];
        add_table_to_tablespace($table, $tbspc_name);
    }
    print_tablespaces_status('------');

    my $i = 0;
    while (1) {
        my $current_result = get_result();
        my @d_tables = grep { !$cant_be_together{$_} } keys %{ $tablespaces{'primary'}->{'tables'} };
        my @l_tables = grep { !$cant_be_together{$_} } keys %{ $tablespaces{'secondary'}->{'tables'} };

        my $primary_table = $d_tables[rand @d_tables];
        my $secondary_table = $l_tables[rand @d_tables];

        my $dto = delete_table_from_tablespace($primary_table, 'primary');
        my $lto = delete_table_from_tablespace($secondary_table, 'secondary');
        add_table_to_tablespace($dto, 'secondary');
        add_table_to_tablespace($lto, 'primary');
        my $new_result = get_result();
        if ($new_result < $current_result) {
            delete_table_from_tablespace($primary_table, 'secondary');
            delete_table_from_tablespace($secondary_table, 'primary');
            add_table_to_tablespace($dto, 'primary');
            add_table_to_tablespace($lto, 'secondary');
        }
        $i++;
        if (0 == $i % 1000) {
            print_tablespaces_status($i);
            open my $fh, ">", "tablespace.split.out" or die "oops?!";
            print_tablespaces_tables($fh);
            close $fh;
            exit if $new_result >= 1.9999;
            next TRY if $i > 20000;
        }
    }
}

exit;

sub print_tablespaces_tables {
    my $fh = shift;
    my $tb_sizes = {};
    for my $t (qw(primary secondary)) {
        my $index_tablespace = ( $t eq 'primary' ? 'secondary' : 'primary' );

        my @ts = sort grep { $_ } keys %{$tablespaces{$t}->{'tables'}};
        for my $table (@ts) {
            $tb_sizes->{$t} += $size_of->{$table};
            printf $fh ("ALTER TABLE %-60s SET TABLESPACE %s;\n", $table, $t);
            for my $index (@{ $indexes_for->{ $table } }) {
                $tb_sizes->{$index_tablespace} += $size_of->{$index};
                printf $fh ("ALTER INDEX %-60s SET TABLESPACE %s;\n", $index, $index_tablespace);
            }
        }
    }
    for my $t (qw(primary secondary)) {
        printf $fh "-- Total size of %-15s : %s\n", $t, $tb_sizes->{$t};
    }
    return;
}

sub get_possible_tablespaces {
    my $table = shift;
    my $bad = $cant_be_together{$table};
    return ('primary', 'secondary') unless $bad;
    if ($tablespaces{'primary'}->{'tables'}->{$bad}) {
        return 'secondary';
    }
    if ($tablespaces{'secondary'}->{'tables'}->{$bad}) {
        return 'primary';
    }
    return ('primary', 'secondary');
}

sub add_table_to_tablespace {
    my ($table, $tablespace_name) = @_;
    my $T = $tablespaces{ $tablespace_name } ||= {};
    $T->{'tables'}->{ $table->{'fullname'} } = $table;
    for my $i (qw(idx_scans writes idx_tup_fetch)) {
        $T->{$i} += $table->{ $i };
    }
    return;
}

sub delete_table_from_tablespace {
    my ($tn, $tspace) = @_;
    my $T = $tablespaces{ $tspace};
    my $table = $T->{'tables'}->{$tn};
    delete $T->{'tables'}->{$tn};
    for my $i (qw(idx_scans writes idx_tup_fetch)) {
        $T->{$i} -= $table->{ $i };
    }
    return $table;
}

sub get_result {
    my $idx_pct = 0;
    if ($tablespaces{'primary'}->{'idx_tup_fetch'} > $tablespaces{'secondary'}->{ 'idx_tup_fetch'}) {
        $idx_pct = $tablespaces{'secondary'}->{ 'idx_tup_fetch'} / $tablespaces{'primary'}->{'idx_tup_fetch'}
    } else {
        $idx_pct = $tablespaces{'primary'}->{ 'idx_tup_fetch'} / $tablespaces{'secondary'}->{'idx_tup_fetch'}
    }
    my $write_pct = 0;
    if ($tablespaces{'primary'}->{'writes'} > $tablespaces{'secondary'}->{ 'writes'}) {
        $write_pct = $tablespaces{'secondary'}->{ 'writes'} / $tablespaces{'primary'}->{'writes'}
    } else {
        $write_pct = $tablespaces{'primary'}->{ 'writes'} / $tablespaces{'secondary'}->{'writes'}
    }
    return $idx_pct + $write_pct;
}

sub print_tablespaces_status {
    my $key = shift;
    printf '%6s: %6s | %15s | %15s ||| %6s | %15s | %15s ||| %8.6f' . "\n",
    $key,
    scalar keys %{$tablespaces{'primary'}->{'tables'}},
    _f($tablespaces{'primary'}->{ 'idx_tup_fetch'}),
    _f($tablespaces{'primary'}->{ 'writes'}),
    scalar keys %{$tablespaces{'secondary'}->{'tables'}},
    _f($tablespaces{'secondary'}->{ 'idx_tup_fetch'}),
    _f($tablespaces{'secondary'}->{ 'writes'}),
    get_result(),
    ;
    return;
}

sub _f {
    my $x = scalar reverse shift;
    $x =~ s/(\d\d\d)(?=\d)/$1 /g;
    return scalar reverse $x;
}

sub get_db_connection {
    return DBI->connect(
        @db_conn_data[0, 1, 2],
        {
            'AutoCommit' => 1,
            'PrintError' => 1,
            'RaiseError' => 1,
            'pg_server_prepare' => 0,
        }
    );
}

sub get_data_from_db {
    my $dbh = get_db_connection();

    my $schema_question_marks = join ',', map { '?' } @USE_SCHEMAS;

    my $data = $dbh->selectall_hashref(
        'SELECT schemaname || ? || relname as fullname, greatest(coalesce(idx_tup_fetch, 0), 0.001) as idx_tup_fetch, greatest( coalesce( n_tup_del + n_tup_ins + n_tup_upd, 0), 0.001) as writes FROM pg_stat_user_tables where schemaname in ' . "( $schema_question_marks )",
        'fullname',
        undef,
        '.',
        @USE_SCHEMAS,
    );
    my $indexes = {};
    my $sth = $dbh->prepare(
        'SELECT schemaname || ? || tablename as tablename, schemaname || ? || indexname as indexname FROM pg_indexes',
    );
    $sth->execute(qw( . . ) );
    while (my $temp = $sth->fetchrow_hashref()) {
        push @{ $indexes->{ $temp->{'tablename'} } }, $temp->{'indexname'};
    }
    $sth->finish;

    $size_of = $dbh->selectall_hashref(
        'SELECT n.nspname || ? || c.relname as fullname, pg_relation_size(c.oid) as size, c.relkind
        FROM pg_class c join pg_namespace n on c.relnamespace = n.oid
        WHERE c.relkind ~ ? and n.nspname in ' . "( $schema_question_marks )",
        'fullname',
        undef,
        '.',
        '^[ri]$',
        @USE_SCHEMAS,
    );

    $dbh->disconnect();

    $indexes_for = $indexes;

    return $data;
}
