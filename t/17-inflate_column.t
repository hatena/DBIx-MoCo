#!perl
use strict;
use warnings;
use FindBin::libs;

ThisTest->runtests;

package ThisTest;
use base qw/Test::Class/;
use Test::More;

use DateTime;
use DateTime::Format::MySQL;

use Blog::Entry;

sub setup : Test(startup) {
    Blog::Entry->inflate_column(
        uri     => 'URI',
        created => {
            inflate => sub { DateTime::Format::MySQL->parse_datetime(shift) },
            deflate => sub { DateTime::Format::MySQL->format_datetime(shift) },
        },
    );
}

sub inflate_by_class_name : Test(6) {
    my $entry = Blog::Entry->retrieve(1);

    isa_ok $entry->uri, 'URI';
    is     $entry->uri->host, 'test.com';

    ## update
    $entry->uri(URI->new_abs('naoya', 'http://d.hatena.ne.jp/'));
    isa_ok $entry->uri, 'URI';
    is     $entry->uri->host, 'd.hatena.ne.jp';

    ## retrieve again
    $entry = Blog::Entry->retrieve(1);
    is     $entry->uri->host, 'd.hatena.ne.jp';

    # set NULL
    $entry->uri(undef);

    $entry = Blog::Entry->retrieve(1);
    ok     !$entry->uri;
}

sub inflate_by_code : Test(5) {
    my $entry = Blog::Entry->retrieve(1);

    isa_ok $entry->created, 'DateTime';
    is     $entry->created->ymd('/'), '2007/03/04';

    ## update
    my $now = DateTime->now(time_zone => 'Asia/Tokyo');
    $entry->created($now);
    isa_ok $entry->created, 'DateTime';
    is     $entry->created->ymd('/'), $now->ymd('/');

    ## retrieve again
    $entry = Blog::Entry->retrieve(1);
    is     $entry->created->ymd('/'), $now->ymd('/');
}
