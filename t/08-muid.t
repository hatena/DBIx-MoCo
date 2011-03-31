#!perl -w
use strict;
use warnings;
use File::Spec;

use lib File::Spec->catdir('t', 'lib');

ThisTest->runtests;

# ThisTest
package ThisTest;
use base qw/Test::Class/;
use Test::More;

sub load : Test(startup => 1) {
    use_ok 'DBIx::MoCo::MUID';
}

sub muid : Test(11) {
    my $muid = DBIx::MoCo::MUID->create_muid;
    ok ($muid, 'muid');
    ok ($muid =~ /^\d+$/, 'is digit');
    ok ($muid < 2 ** 64, 'less than 2 ** 64');
    # ok ($muid > 2 ** 63, 'greater than 2 ** 63');
    # ok ($muid > 2 ** 56, 'greater than 2 ** 56');
    ok ($muid > 2 ** 44, 'greater than 2 ** 44');

    my $muid2 = create_muid();
    ok ($muid2, 'muid2');
    ok ($muid2 =~ /^\d+$/, 'is digit');
    ok ($muid2 < 2 ** 64, 'less than 2 ** 64');
    # ok ($muid2 > 2 ** 63, 'greater than 2 ** 63');
    # ok ($muid2 > 2 ** 56, 'greater than 2 ** 56');
    ok ($muid2 > 2 ** 44, 'greater than 2 ** 44');
    ok ($muid ne $muid2, '1 ne 2');
    my $muid3 = create_muid();
    ok ($muid ne $muid3, '1 ne 3');
    ok ($muid2 ne $muid3, '2 ne 3');
}

sub _get_ipaddr : Test(2) {
    ok DBIx::MoCo::MUID::_get_ipaddr();

    no warnings qw(once redefine);
    eval { require Net::Address::IP::Local };
    local *Net::Address::IP::Local::public_ipv4 = sub { die };
    local *DBIx::MoCo::MUID::get_addresses      = sub { die };

    my $warn;
    local $SIG{__WARN__} = sub { $warn = $_[0] };
    is DBIx::MoCo::MUID::_get_ipaddr(), '0.0.0.0';
    note $warn;
}

1;
