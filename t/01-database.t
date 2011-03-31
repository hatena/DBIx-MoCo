#!perl -T
use strict;
use warnings;
use File::Spec;

use lib File::Spec->catdir('t', 'lib');

ThisTest->runtests;

# ThisTest
package ThisTest;
use base qw/Test::Class/;
use Test::More;
use Test::Exception;
use File::Spec;
use IO::Handle;

sub use_test : Test(startup => 2) {
    use_ok 'DBIx::MoCo::DataBase';
    use_ok 'Blog::DataBase';
}

sub setup : Test(setup) {
    Blog::DataBase->last_sth(undef);
}

sub dbh : Tests {
    my $dbh = Blog::DataBase->dbh;
    ok $dbh;
    isa_ok $dbh, 'DBI::db';
    my $dbh2 = Blog::DataBase->dbh;
    is $dbh, $dbh2;
}

sub execute : Tests {
    my $data;
    my $res = Blog::DataBase->execute('select 1', \$data);
    ok $res;
    ok $data;
    isa_ok $data, 'ARRAY';
    is scalar(@$data), 1;
    isa_ok $data->[0], 'HASH';
    is $data->[0]->{1}, 1;
    isa_ok +Blog::DataBase->last_sth, 'DBI::st', 'DataBase->last_sth';
    is +Blog::DataBase->last_sth->rows, 1, 'DataBase->last_sth->rows';
    is +Blog::DataBase->last_rows, 1, 'DataBase->last_rows';
    is +Blog::DataBase->last_rows, $DBI::rows;
}

sub select : Tests {
    my $data;
    my $sql = 'select * from user';
    my $res = Blog::DataBase->execute($sql,\$data);
    ok $res;
    ok $data;
    isa_ok $data, 'ARRAY';
    my $u = $data->[0];
    isa_ok $u, 'HASH';
    ok $u->{name};
    ok $u->{user_id};
}

sub prepared_cached : Tests {
    my $sql = 'select * from user where user_id = ?';

    my $kids = Blog::DataBase->dbh->{CachedKids};
    %$kids = ();

    {
        ok (Blog::DataBase->execute($sql, \my $data, [1]));
        ok $data;
        isa_ok $data, 'ARRAY';

        my $u = $data->[0];
        isa_ok $u, 'HASH';
        is $u->{user_id}, 1;
        is $u->{name}, 'jkondo';
        is scalar keys %$kids, 1;
        isa_ok((values %$kids)[0], 'DBI::st');
    }

    {
        ok (Blog::DataBase->execute($sql, \my $data, [2]));
        is scalar keys %$kids, 1;
        my @sts = values %$kids;
        isa_ok((values %$kids)[0], 'DBI::st');

        my $u = $data->[0];
        is $u->{user_id}, 2;
    }
}

sub reconnect : Tests {
    Blog::DataBase->dbh->disconnect;
    my $data;
    my $res = Blog::DataBase->execute('select 1', \$data);
    ok $res;
    ok $data;
}

sub replicatioin : Tests {
    my $db = 'TestDataBase';
    $db->dsn('dbi:mysql:dbname=test');
    is ($db->get_dsn, 'dbi:mysql:dbname=test', 'single string dsn');
    $db->dsn(['dbi:mysql:dbname=test;host=db1','dbi:mysql:dbname=test;host=db2']);
    is ($db->get_dsn, 'dbi:mysql:dbname=test;host=db1', 'single array dsn 1');
    is ($db->get_dsn, 'dbi:mysql:dbname=test;host=db2', 'single array dsn 2');
    is ($db->get_dsn, 'dbi:mysql:dbname=test;host=db1', 'single array dsn 3');
    $db->dsn(
        master => ['dbi:mysql:dbname=test;host=db1','dbi:mysql:dbname=test;host=db2'],
    );
    is ($db->get_dsn, 'dbi:mysql:dbname=test;host=db1', 'master array dsn 1');
    is ($db->get_dsn, 'dbi:mysql:dbname=test;host=db2', 'master array dsn 2');
    is ($db->get_dsn, 'dbi:mysql:dbname=test;host=db1', 'master array dsn 3');
    is ($db->get_dsn('SELECT 1'), 'dbi:mysql:dbname=test;host=db2', 'master array select');
    is ($db->get_dsn('INSERT 1'), 'dbi:mysql:dbname=test;host=db1', 'master array insert');
    $db->dsn(
        master => 'dbi:mysql:dbname=test;host=db1',
        slave => ['dbi:mysql:dbname=test;host=db2','dbi:mysql:dbname=test;host=db3'],
    );
    is ($db->get_dsn, 'dbi:mysql:dbname=test;host=db1', 'master-slave no sql');
    is ($db->get_dsn, 'dbi:mysql:dbname=test;host=db1', 'master-slave no sql 2');
    is ($db->get_dsn('SELECT 1'), 'dbi:mysql:dbname=test;host=db2', 'master-slave select sql 1');
    is ($db->get_dsn('SELECT 1'), 'dbi:mysql:dbname=test;host=db3', 'master-slave select sql 2');
    is ($db->get_dsn('SELECT 1'), 'dbi:mysql:dbname=test;host=db2', 'master-slave select sql 3');
    is ($db->get_dsn('INSERT 1'), 'dbi:mysql:dbname=test;host=db1', 'master-slave insert sql 1');
    is ($db->get_dsn('INSERT 1'), 'dbi:mysql:dbname=test;host=db1', 'master-slave insert sql 2');
    $db->dsn({
        master => 'dbi:mysql:dbname=test;host=db1',
        slave => ['dbi:mysql:dbname=test;host=db2','dbi:mysql:dbname=test;host=db3'],
    });
    is ($db->get_dsn, 'dbi:mysql:dbname=test;host=db1', 'master-slave no sql');
    is ($db->get_dsn, 'dbi:mysql:dbname=test;host=db1', 'master-slave no sql 2');
    is ($db->get_dsn('SELECT 1'), 'dbi:mysql:dbname=test;host=db2', 'master-slave select sql 1');
    is ($db->get_dsn('SELECT 1'), 'dbi:mysql:dbname=test;host=db3', 'master-slave select sql 2');
    is ($db->get_dsn('SELECT 1'), 'dbi:mysql:dbname=test;host=db2', 'master-slave select sql 3');
    is ($db->get_dsn('INSERT 1'), 'dbi:mysql:dbname=test;host=db1', 'master-slave insert sql 1');
    is ($db->get_dsn('INSERT 1'), 'dbi:mysql:dbname=test;host=db1', 'master-slave insert sql 2');
}

sub raise_error : Tests {
    open my $stderr, ">&", STDERR;
    open STDERR, '>', File::Spec->devnull;

    my $default = Blog::DataBase->raise_error;
    Blog::DataBase->raise_error(1);
    dies_ok {Blog::DataBase->execute('select broken sql')} 'MoCo dies';
    Blog::DataBase->raise_error(0);
    # lives_ok {Blog::DataBase->execute('select broken sql')} 'MoCo lives';
    # Blog::DataBase->raise_error($default);

    open STDERR, '>&', $stderr;
}

our $fake_time;
BEGIN {
    *CORE::GLOBAL::time = sub { $fake_time }
}

sub current_timestamp : Tests {
    my $real_time = CORE::time(); # == database's NOW()

    $fake_time = $real_time;
    is Blog::DataBase->current_timestamp, $real_time;
    is Blog::DataBase->current_timestamp, $real_time;
    is Blog::DataBase->current_timestamp(update => 1), $real_time;
    is Blog::DataBase->current_timestamp, $real_time;

    $fake_time = 1;
    is Blog::DataBase->current_timestamp(update => 1), $real_time;
    is Blog::DataBase->current_timestamp, $real_time;
    is Blog::DataBase->current_timestamp, $real_time;
}

1;

# TestDataBase
package TestDataBase;
use base qw(DBIx::MoCo::DataBase);

1;
