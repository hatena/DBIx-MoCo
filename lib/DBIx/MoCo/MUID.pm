package DBIx::MoCo::MUID;
use strict;
use Exporter qw(import);
our @EXPORT = qw(create_muid);

use Math::BigInt;
use Time::HiRes;

our ($ser, $addr);

$addr = substr(join('', map { sprintf('%08b', $_) } split(/\./, _get_ipaddr())), -20);

sub _get_ipaddr {
    my $addr;

    if (!$addr) {
        eval {
            require Net::Address::IP::Local;
            $addr = Net::Address::IP::Local->public_ipv4;
        };
    }

    if (!$addr) {
        eval {
            require Net::Address::Ethernet;
            $addr = (Net::Address::Ethernet::get_addresses())[0]->{sIP};
        };
    }

    if (!$addr) {
        warn "Could not get network I/F address; fallback to 0.0.0.0";
        $addr = '0.0.0.0';
    }

    return $addr;
}

sub create_muid {
    unless (defined $ser) {
        $ser = int(rand(256));
    }
    # my $time = sprintf('%032b', time());
    Math::BigInt->new(int(Time::HiRes::time() * 1000))->as_bin =~ /([01]{36})$/o;
    my $time = $1;
    my $serial = sprintf('%08b', $ser++ % 256);
    my $muid = $addr . $time . $serial;
    $muid = Math::BigInt->new("0b$muid");
    # warn $muid->bstr();
    # warn $muid->as_bin();
    return $muid->bstr();
}

1;

=head1 NAME

DBIx::MoCo::MUID - MUID generator for muid fields

=head1 SYNOPSIS

  my $muid = DBIx::MoCo::MUID->create_muid();

=head1 DESCRIPTION

I<DBIx::MoCo::MUID> provides "almost unique" id for MoCo Unique ID 
(muid) fields.
They are less unique than UUIDs because they only have 64bits long.

They are generated as set of next 3 parts.

20 bits of ip address (last 20 bits)
36 bits of epoch time (lower 36 bits of msec.) (2.179 years)
8 bits of serial

=head1 SEE ALSO

L<DBIx::MoCo>

=head1 AUTHOR

Junya Kondo, E<lt>jkondo@hatena.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) Hatena Inc. All Rights Reserved.

This library is free software; you may redistribute it and/or modify
it under the same terms as Perl itself.

=cut
