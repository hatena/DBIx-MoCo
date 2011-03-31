package DBIx::MoCo::DataBase;
use strict;
use warnings;
use Carp;
use base qw (Class::Data::Inheritable);
use DBI;
use SQL::Abstract;
use Log::Dispatch::Screen;
use Time::HiRes;
use UNIVERSAL::require;
use DBIx::MoCo::DataBase::MasterGuardian;

__PACKAGE__->mk_classdata($_) for qw(username password options auto_commit raise_error __logger coop_debug_logger
                                     cache_connection last_insert_id use_master_flag last_sth last_rows);
__PACKAGE__->coop_debug_logger(1);
__PACKAGE__->raise_error(1);
__PACKAGE__->cache_connection(1);
__PACKAGE__->use_master_flag(0);

use constant SLOW_LOG => 0.1;

use constant COMPILE_TIME_DEBUG => defined $ENV{MOCO_DEBUG};

tie our $DEBUG, 'DBIx::MoCo::DataBase::Debug';
__PACKAGE__->logger(Log::Dispatch::Screen->new(name => 'screen', min_level => 'info', stderr => 1));
#my $logger = DBIx::MoCo::Logger->new;
our $SQL_COUNT = 0;

# $Carp::CarpLevel = 2;
my $sqla = SQL::Abstract->new;

sub __sqla { $sqla }

sub insert {
    my $class = shift;
    my ($table, $args) = @_;
    my ($sql, @binds) = $sqla->insert($table,$args);
    $class->execute($sql,undef,\@binds);
}

sub use_master {
    my $class = shift;
    $class->use_master_flag(1);
}

sub no_use_master {
    my $class = shift;
    $class->use_master_flag(0);
}

sub ensure_master {
    my ($class) = @_;
    DBIx::MoCo::DataBase::MasterGuardian->new($class)
}

sub delete {
    my $class = shift;
    my ($table, $where) = @_;
    $where or croak "where is not specified to delete from $table";
    (ref $where eq 'HASH' && %$where) or croak "where is not specified to delete from $table";
    my ($sql, @binds) = $sqla->delete($table,$where);
    $sql =~ /WHERE/io or croak "where is not specified to delete from $table";
    $class->execute($sql,undef,\@binds);
}

sub update {
    my $class = shift;
    my ($table, $args, $where) = @_;
    $where or croak "where is not specified to update $table";
    (ref $where eq 'HASH' && %$where) or croak "where is not specified to update $table";
    my ($sql, @binds) = $sqla->update($table,$args,$where);
    $sql =~ /WHERE/io or croak "where is not specified to update $table";
    $class->execute($sql,undef,\@binds);
}

sub select {
    my $class = shift;
    my ($table, $args, $where, $order, $limit) = @_;
    my ($sql, @binds) = $sqla->select($table,$args,$where,$order);
    $sql .= $class->_parse_limit($limit) if $limit;
    my $data;
    $class->execute($sql,\$data,\@binds) or return;
    return $data;
}

sub search {
    my $class = shift;
    my %args = @_;
    my ($sql, @binds) = $class->_search_sql(\%args);
    my $data;
    $class->execute($sql,\$data,\@binds, $args{use_master}) or return;
    return $data;
}

sub _search_sql {
    my $class = shift;
    my $args = shift;
    my $field = $args->{field} || "*";
    my $sql = "SELECT $field FROM " . $args->{table};
    $sql .= " USE INDEX ($args->{use_index})" if $args->{use_index};
    $sql .= " FORCE INDEX ($args->{force_index})" if $args->{force_index};
    my ($where,@binds) = $class->_parse_where($args->{where});
    $sql .= $where if $where;
    $sql .= " GROUP BY $args->{group}" if $args->{group};
    $sql .= " ORDER BY $args->{order}" if $args->{order};
    $sql .= $class->_parse_limit($args);
    return ($sql,@binds);
}

sub _parse_where {
    my ($class, $where) = @_;
    my $binds = [];
    if (ref $where eq 'ARRAY') {
        my ($sql, @args) = @$where;
        if ($sql =~ m!\s*:[A-Za-z_][A-Za-z0-9_]+\s*!o) {
            @args % 2 and croak "You gave me an odd number of parameters to 'where'!";
            my %named_values = @args;
            my @values;
            $sql =~ s{:([A-Za-z_][A-Za-z0-9_]*)}{
                croak "$1 is not exists in hash" if !exists $named_values{$1};
                my $value = $named_values{$1};
                if (ref $value eq 'ARRAY') {
                    push @values, $_ for @$value;
                    join ',', map('?', 1..@$value);
                } else {
                    push @values, $value;
                    '?'
                }
            }ge;
            $binds = \@values;
        } else {
            $binds = \@args;
        }
        return (' WHERE ' . $sql, @$binds);
    } elsif (ref $where eq 'HASH') {
        return $sqla->where($where);
    } elsif ($where) {
        return ' WHERE ' . $where;
    }
    return $where;
}

sub _parse_limit {
    my ($class, $args) = @_;
    my $sql = '';
    if ($args->{offset} || $args->{limit}) {
        $sql .= " LIMIT ";
        if ($args->{offset} && $args->{offset} =~ m/^\d+$/o) {
            $sql .= $args->{offset}.",";
        }
        $sql .= $args->{limit} =~ /^\d+$/o ? $args->{limit} : '1';
    }
    return $sql;
}

our $timestamp_diff;

sub current_timestamp {
    my ($class, %args) = @_;

    if (defined $timestamp_diff && !$args{update}) {
        return time() + $timestamp_diff;
    } else {
        my $time = $class->_get_current_unix_timestamp;
        $timestamp_diff = $time - time();
        return $time;
    }
}

sub _get_current_unix_timestamp {
    my $class = shift;

    my $data;
    my $vendor = $class->vendor;
    if ($vendor eq 'MySQL') {
        $class->execute('SELECT UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) AS time', \$data) or die;
    } elsif ($vendor eq 'SQLite') {
        $class->execute(q#SELECT STRFTIME('%s', 'now') AS time#, \$data) or die;
    } else {
        croak(qq(_get_current_unix_timestamp not implemented for vendor '$vendor'));
    }
    return $data->[0]->{time};
}

sub dsn {
    my $class = shift;
    my ($master_dsn, $slave_dsn);
    if ($_[0] && ref($_[0]) eq 'HASH') {
        @_ = (%{$_[0]});
    }
    if ($_[1]) {
        my %args = @_;
        my $master = $args{master} or croak "master dsn is not specified";
        $master_dsn = ref($master) eq 'ARRAY' ? $master : [$master];
        my $slave = $args{slave} || $master;
        $slave_dsn = ref($slave) eq 'ARRAY' ? $slave : [$slave];
    } elsif ($_[0]) {
        $slave_dsn = $master_dsn = ref($_[0]) eq 'ARRAY' ? $_[0] : [$_[0]];
    } else {
        croak "Please specify your dsn.";
    }
#     $dsn->{$class} = {
#         master => $master_dsn,
#         slave => $slave_dsn,
#     };
    my $getter = $class . '::get_dsn';
    {
        no strict 'refs';
        no warnings 'redefine';
        *{$getter} = sub {
            my $class = shift;
            my $sql = shift;
            my $args = shift || {};
            my $list = $master_dsn;
            if (!$args->{use_master} && !$class->use_master_flag && $sql && $sql =~ /^SELECT/io) {
                $list = $slave_dsn 
            }
            my $dsn = shift @$list;
            push @$list, $dsn;
            return $dsn;
        }
    }
}

sub get_dsn { croak "You must set up your dsn first" }

my %USERNAME;
my %PASSWORD;
my %RAISE_ERROR;
my %AUTO_COMMIT;
my %CACHE_CONNECTION;
my %OPTIONS;
sub dbh {
    my $class = shift;
    my $sql   = shift;
    my $args  = shift || {};
    my $dsn = $class->get_dsn($sql, $args);

    my $key = join '-', $dsn, $class;

    if (not defined $CACHE_CONNECTION{$key}) {
        $CACHE_CONNECTION{$key} = $class->cache_connection ? 1 : 0;
    }

    if (not defined $RAISE_ERROR{$key}) {
        $RAISE_ERROR{$key} = $class->raise_error ? 1 : 0;
    }

    if (not defined $AUTO_COMMIT{$key}) {
        if (defined $class->auto_commit) {
            $AUTO_COMMIT{$key} = $class->auto_commit ? 1 : 0;
        } else {
            $AUTO_COMMIT{$key} = 2; # not set OPTIONS
        }
    }

    my $connect = $CACHE_CONNECTION{$key} ? 'connect_cached' : 'connect';


    $OPTIONS{$key}  ||= $class->options || {};
    $OPTIONS{$key}->{RaiseError} = $RAISE_ERROR{$key} ? 1 : 0;
    unless ($AUTO_COMMIT{$key} == 2) {
        $OPTIONS{$key}->{AutoCommit} = $AUTO_COMMIT{$key} ? 1 : 0;
    }
    $USERNAME{$key} ||= $class->username;
    $PASSWORD{$key} ||= $class->password;

    DBI->$connect($dsn, $USERNAME{$key}, $PASSWORD{$key}, $OPTIONS{$key});
}

my %LOGGER;

sub logger {
    my $class = shift;
    if (@_) {
        $class->__logger(@_);
        for (keys %LOGGER) {
            delete $LOGGER{$_};
        }
    } else {
        return $LOGGER{$class} ||= $class->__logger;
    }
}

our %SQLSeen;
our %SQLAndValuesSeen;
our $DEBUG_CALLER_IGNORE_RE ||= qr/\bDBIx?\b/;

if ($ENV{MOCO_DEBUG_CALLER_IGNORE_RE}) {
    $DEBUG_CALLER_IGNORE_RE = qr/$ENV{MOCO_DEBUG_CALLER_IGNORE_RE}/o;
}

sub execute {
    my $class = shift;
    my ($sql, $data, $binds, $use_master) = @_;
    $sql or return;
    my @bind_values = ref $binds eq 'ARRAY' ? @$binds : ();
    my $dbh = $class->dbh(substr($sql,0,8), { use_master => $use_master ? 1 : 0 });

    if (COMPILE_TIME_DEBUG && $DEBUG && $DEBUG =~ /TRACE/i) {
        my @message;
        my $count = 0;
        for (my $n = 1; $n < 20; $n++) {
            my ($pkg, undef, $line, $sub) = caller($n) or last;
            next if $DEBUG_CALLER_IGNORE_RE && $pkg =~ $DEBUG_CALLER_IGNORE_RE;
            push @message, "$sub at $line";
        }
        __PACKAGE__->logger->log(level => 'info', message => "\n  " . (join ' < ', @message) . "\n");
    }

    my $sth = @bind_values ? $dbh->prepare_cached($sql,undef,1) :
        $dbh->prepare($sql);
    unless ($sth) { carp $dbh->errstr and return; }

    $class->last_sth($sth);

    my @binds = map { defined $_ ? "'$_'" : "'NULL'" } @bind_values; 

    if ($DEBUG) {
        $SQL_COUNT++;
    }

    if (COMPILE_TIME_DEBUG && $DEBUG && $DEBUG =~ /COMMENT/i) {
        for (my $n = 1; $n < 20; $n++) {
            my ($pkg, undef, $line, $sub) = caller($n) or last;
            next if $DEBUG_CALLER_IGNORE_RE && $pkg =~ $DEBUG_CALLER_IGNORE_RE;
            $sql = "/* $pkg line $line */ $sql";
            last;
        }
    }

    $LOGGER{$class} ||= $class->logger;
    my $sql_error = sub {
        my ($sql, $sth) = @_;
        sprintf 'SQL Error: "%s" (%s)', $sql, $sth->errstr;
    };

    my $bm_time = '';
    my $debug = $DEBUG || ($LOGGER{$class}->min_level eq 'debug');
    if ($debug) {
        $bm_time = [Time::HiRes::gettimeofday];
    }

    eval {
        if (defined $data) {
            $sth->execute(@bind_values) or carp $sql_error->($sql, $sth) and return;
            $$data = $sth->fetchall_arrayref({});
        } else {
            unless ($sth->execute(@bind_values)) {
                carp $sql_error->($sql, $sth);
                return;
            }
        }
    };
    {
        local $@;
        my $extra = '';
        if ($debug) {
            my $itv = Time::HiRes::tv_interval($bm_time);

            $bm_time = sprintf "%.2f ms",  $itv * 1000;
            $bm_time .= ' [SLOW]' if ($itv > SLOW_LOG);

            if (COMPILE_TIME_DEBUG && $debug =~ /EXPLAIN/i && $sql =~ /^SELECT/i) {
                my $sth = $dbh->prepare("EXPLAIN $sql");
                $sth->execute(@bind_values);
                my $data = $sth->fetchall_arrayref({});
                if (grep { ($_->{Extra} || '') =~ /filesort|temporary/ } @$data) {
                    if (Text::ASCIITable->require) {
                        my $t = Text::ASCIITable->new;
                        my @cols = qw(id select_type table type possible_keys key key_len ref rows Extra);
                        $t->setCols(@cols);
                        $t->addRow(@$_{@cols}) foreach @$data;
                        $extra .= "\n" . $t;
                    } else {
                        warn $@;
                    }
                }
            }
            if ($debug =~ /EXPLAIN/i) {
                $extra .= sprintf ' (%s%s)', ++$SQLSeen{$sql}, $SQLAndValuesSeen{"$sql,@bind_values"}++ ? ', seen query' : '';
            }

            my $caller_info = '';
            for (my $n = 1; $n < 10; $n++) {
                my ($pkg, $file, $line) = caller($n) or last;
                next if $DEBUG_CALLER_IGNORE_RE && $pkg =~ $DEBUG_CALLER_IGNORE_RE;
                $caller_info = "$file:$line";
                last;
            }

            __PACKAGE__->logger->log(
                level => 'debug',
                message => "$bm_time | $sql (@binds) | $class | $caller_info$extra\n",
            );
        }

    }
    if ($@) {
        confess $sql_error->($sql, $sth);
    }

    if ($sql =~ /^insert/io) {
        $class->last_insert_id($dbh->last_insert_id(undef,undef,undef,undef) ||
                           $dbh->{'mysql_insertid'});
    }
    $class->last_rows($sth->rows);

    return !$sth->err;
}

sub vendor {
    my $class = shift;
    $class->dbh->get_info(17); # SQL_DBMS_NAME
}

sub primary_keys {
    my $class = shift;
    my $table = shift or return;
    my $dbh = $class->dbh;
    if ($class->vendor eq 'MySQL') {
        my $sth = $dbh->column_info(undef,undef,$table,'%') or
            croak $dbh->errstr;
        $dbh->err and croak $dbh->errstr;
        my @cols = @{$sth->fetchall_arrayref({})} or
            croak "Table '$table': could not get primary keys";
        return [
            map {$_->{COLUMN_NAME}}
            grep {$_->{mysql_is_pri_key}}
            @cols
        ];
    } else {
        return [$dbh->primary_key(undef,undef,$table)];
    }
}

sub unique_keys {
    my $class = shift;
    my $table = shift or return;
    if ($class->vendor eq 'MySQL') {
        my $sql = "SHOW INDEX FROM $table";
        my $data;
        $class->execute($sql,\$data) or
            croak "Table '$table': could not get unique keys";
        @$data or croak "Table '$table': could not get unique keys";
        return [
            map {$_->{Column_name}}
            grep {!$_->{Non_unique}}
            @$data
        ];
    } else {
        return $class->primary_keys($table);
    }
}

sub columns {
    my $class = shift;
    my $table = shift or return;
    my $dbh = $class->dbh;
    if (my $sth = $class->dbh->column_info(undef,undef,$table,'%')) {
        croak $dbh->errstr if $dbh->err;
        my @cols = @{$sth->fetchall_arrayref({})} or
            croak "Table '$table': could not get primary keys";
        return [
            map {$_->{COLUMN_NAME}}
            @cols
        ];
    } else {
        my $d = $class->select($table,'*',undef,'',{limit => 1}) or return;
        return [keys %{$d->[0]}];
    }
}

package DBIx::MoCo::DataBase::Debug;

sub TIESCALAR {
    bless \do { my $o = $ENV{MOCO_DEBUG} }, shift;
}

sub FETCH($) {
    my $this = shift;
    return $$this;
}

sub STORE($$) {
    my ($this, $value) = @_;
    if (DBIx::MoCo::DataBase->coop_debug_logger) {
        DBIx::MoCo::DataBase->logger->{min_level} = $value ? 0 : 1; # debug / info
    }
    $$this = $value;
}

sub DESTROY {
}
1;

=head1 NAME

DBIx::MoCo::DataBase - Data Base Handler for DBIx::MoCo

=head1 SYNOPSIS

  package MyDataBase;
  use base qw(DBIx::MoCo::DataBase);

  __PACKAGE__->dsn('dbi:mysql:myapp');
  __PACKAGE__->username('test');
  __PACKAGE__->password('test');
  __PACKAGE__->logger(Log::Dispatch->new(...));

  1;

  # In your scripts
  MyDataBase->execute('select 1');

  # Configure your replication databases
  __PACKAGE__->dsn(
    master => 'dbi:mysql:dbname=test;host=db1',
    slave => ['dbi:mysql:dbname=test;host=db2','dbi:mysql:dbname=test;host=db3'],
  );

=head1 METHODS

=over 4

=item cache_connection

Controlls cache behavior for dbh connection. (default 1)
If its set to 0, DBIx::MoCo::DataBase uses DBI->connect instead of
DBI->connect_cached.

  DBIx::MoCo::DataBase->cache_connection(0);

=item dsn

Configures dsn(s). You can specify single dsn as string, multiple dsns as an array,
master/slave dsns as hash.

If you specify multiple dsns, they will be rotated automatically in round-robin.
MoCo will use slave dsns when the sql begins with C<SELECT> if you set up slave(s).

  MyDataBase->dsn('dbi:mysql:dbname=test');
  MyDataBase->dsn(['dbi:mysql:dbname=test;host=db1','dbi:mysql:dbname=test;host=db2']);
  MyDataBase->dsn(
     master => ['dbi:mysql:dbname=test;host=db1','dbi:mysql:dbname=test;host=db2'],
  );
  MyDataBase->dsn(
    master => 'dbi:mysql:dbname=test;host=db1',
    slave => ['dbi:mysql:dbname=test;host=db2','dbi:mysql:dbname=test;host=db3'],
  );

=item raise_error

Switches DBI's RaiseError option. (default 1)

  MyDataBase->raise_error(0);

=item coop_debug_logger

$DEBUG flag cooperative logger option. (default 1)

  MyDataBase->coop_debug_logger(0);

=back

=head1 SEE ALSO

L<DBIx::MoCo>, L<SQL::Abstract>

=head1 AUTHOR

Junya Kondo, E<lt>jkondo@hatena.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) Hatena Inc. All Rights Reserved.

This library is free software; you may redistribute it and/or modify
it under the same terms as Perl itself.

=cut
