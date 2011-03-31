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
use Blog::Entry;
use Blog::User;
use Storable qw(nfreeze);
use Data::Dumper;

sub has_a : Tests {
    my $e = Blog::Entry->retrieve(1);
    ok ($e, 'retrieve entry');
    isa_ok ($e, 'Blog::Entry');
    my $len = length(nfreeze($e));
    ok ($len, 'length of freeze');
    my $u = $e->user;
    ok ($u, 'entry->user');
    isa_ok ($u, 'Blog::User');
    my $len2 = length(nfreeze($e));
    ok ($len == $len2, 'same length');
}

sub has_many : Tests {
    my $u = Blog::User->retrieve(1);
    ok ($u, 'retrieve user');
    isa_ok ($u, 'Blog::User');
    # warn Dumper($u);
    my $len = length(nfreeze($u));
    ok ($len, 'length of freeze');
    # warn $len;
    my $entries = $u->entries(0,1);
    ok ($entries, 'entries');
    my $e1 = $entries->first;
    ok ($e1, 'first entry');
    isa_ok ($e1, 'Blog::Entry');
    # warn Dumper($u);
    my $len2 = length(nfreeze($u));
    ok ($len2, 'length of freeze');
    # warn $len2;
    ok ($len2 < $len + 80, 'length 2 is not so long');
    $entries = $u->entries(0,2);
    ok ($entries, 'entries');
    is_deeply ($entries->first, $e1, 'same first');
    my $e2 = $entries->last;
    isa_ok ($e2, 'Blog::Entry');
    # warn Dumper($u);
    my $len3 = length(nfreeze($u));
    ok ($len3, 'length of freeze');
    # warn $len3;
    ok ($len3 < $len + 100, 'length 3 is not so long');
    $entries = $u->entries(0,2);
    is_deeply ($entries->last, $e2, 'same last entry');
}

1;
