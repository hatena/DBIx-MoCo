#!/usr/bin/env perl
use strict;
use warnings;
use FindBin::libs;

ThisTest->runtests;

package Blog::Entry::List;
use base qw/DBIx::MoCo::List/;

sub size_test {
    shift->size;
}

package ThisTest;
use base qw/Test::Class/;
use Test::More;
use Blog::Entry;

sub scalar_test : Tests {
    my $entries = Blog::Entry->scalar(
        'search',
        limit => 3
    );
    isa_ok $entries, 'DBIx::MoCo::List';
    isa_ok $entries, 'DBIx::MoCo::List';
    is $entries->size, 3;

    my $entry = $entries->first;
    my @bookmarks = $entry->scalar('bookmarks', 0, 1);
    isa_ok $bookmarks[0], 'DBIx::MoCo::List';
    is $bookmarks[0]->size, 1;
}
