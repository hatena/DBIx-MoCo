use strict;
use warnings;
use File::Spec;

use lib File::Spec->catdir('t', 'lib');

ThisTest->runtests;

# ThisTest
package ThisTest;
use base qw/Test::Class/;

use Test::More;
# use DBIx::MoCo::DataBase;
# use Blog::DataBase;
use File::Spec;
use IO::String;
use Log::Dispatch::Handle;
use B::Deparse;

sub use_test : Test(startup => 2) {
    local $ENV{MOCO_DEBUG};
    use_ok 'DBIx::MoCo::DataBase';
    use_ok 'Blog::DataBase';
}

sub logger_levels : Test(5) {
    isa_ok(DBIx::MoCo::DataBase->logger, 'Log::Dispatch::Screen');
    is(DBIx::MoCo::DataBase->logger->{min_level}, 1);
    local $DBIx::MoCo::DataBase::DEBUG = 1;
    is(DBIx::MoCo::DataBase->logger->{min_level}, 0);
    local $DBIx::MoCo::DataBase::DEBUG = 0;
    is(DBIx::MoCo::DataBase->logger->{min_level}, 1);
    DBIx::MoCo::DataBase->coop_debug_logger(0);
    local $DBIx::MoCo::DataBase::DEBUG = 1;
    is(DBIx::MoCo::DataBase->logger->{min_level}, 1);
}

sub logging : Test(3) {
    my $io = IO::String->new;

    my $logger = Log::Dispatch::Handle->new(name => 'handle', min_level => 'debug', handle => $io);
    isa_ok(my $old_logger = DBIx::MoCo::DataBase->logger, 'Log::Dispatch::Screen');

    DBIx::MoCo::DataBase->logger($logger);

    isa_ok(DBIx::MoCo::DataBase->logger, 'Log::Dispatch::Handle');
    Blog::DataBase->execute('select 1', \my $data);
    like ${$io->string_ref} , qr/select 1 \(\)/;

    DBIx::MoCo::DataBase->logger($old_logger);
}

sub _reload_database_pm {
    undef $_ for values %{$::{'DBIx::'}{'MoCo::'}{'DataBase::'}};
    do 'DBIx/MoCo/DataBase.pm';
}

sub env_moco_debug : Test(7) {
    ok !DBIx::MoCo::DataBase::COMPILE_TIME_DEBUG();

    {
        local $ENV{MOCO_DEBUG} = 'EXPLAIN,TRACE,COMMENT';

        _reload_database_pm();

        my $io = IO::String->new;
        my $logger = Log::Dispatch::Handle->new(name => 'handle', min_level => 'debug', handle => $io);
        DBIx::MoCo::DataBase->logger($logger);

        ok  DBIx::MoCo::DataBase::COMPILE_TIME_DEBUG();
        is $DBIx::MoCo::DataBase::DEBUG, 'EXPLAIN,TRACE,COMMENT';

        Blog::DataBase->execute('SELECT * FROM user');
        like ${ $io->string_ref }, qr(/\* Test::Class line \d+ \*/ SELECT \* FROM user);
        note ${ $io->string_ref };
        note +B::Deparse->new->coderef2text(\&DBIx::MoCo::DataBase::execute);
    }
    
    {
        local $ENV{MOCO_DEBUG};

        _reload_database_pm();

        my $io = IO::String->new;
        my $logger = Log::Dispatch::Handle->new(name => 'handle', min_level => 'debug', handle => $io);
        DBIx::MoCo::DataBase->logger($logger);

        ok !DBIx::MoCo::DataBase::COMPILE_TIME_DEBUG();
        is $DBIx::MoCo::DataBase::DEBUG, undef;

        Blog::DataBase->execute('SELECT * FROM user');
        unlike ${ $io->string_ref }, qr(/\* Test::Class line \d+ \*/ SELECT \* FROM user);
        note ${ $io->string_ref };
        note +B::Deparse->new->coderef2text(\&DBIx::MoCo::DataBase::execute);
    }

    _reload_database_pm();
}

1;
