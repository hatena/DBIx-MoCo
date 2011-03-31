use lib 'lib';
use DBIx::MoCo::DataBase;

use Test::More;

subtest basic => sub {
    ok !DBIx::MoCo::DataBase->use_master_flag;
    {
        my $guard = DBIx::MoCo::DataBase->ensure_master;
        ok !!DBIx::MoCo::DataBase->use_master_flag;
    };
    ok !DBIx::MoCo::DataBase->use_master_flag;
    done_testing;
};

subtest die => sub {
    ok !DBIx::MoCo::DataBase->use_master_flag;
    eval {
        my $guard = DBIx::MoCo::DataBase->ensure_master;
        ok !!DBIx::MoCo::DataBase->use_master_flag;
        die;
    };
    ok !DBIx::MoCo::DataBase->use_master_flag;
    done_testing;
};

done_testing;
