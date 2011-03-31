package DBIx::MoCo::Fixture::Hash;
use strict;
use warnings;
use Tie::Hash;
use base qw/Tie::StdHash/;

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    tie %$self, 'DBIx::MoCo::Fixture::Hash';
    return $self;
}

sub DESTROY {};

our $AUTOLOAD;
sub AUTOLOAD : lvalue {
    (my $key = $AUTOLOAD) =~ s!.+::!!;

    {
        no strict 'refs';
        *$AUTOLOAD = sub : lvalue { $_[0]->{$key} };
    }

    goto &$AUTOLOAD;

    $_[0]->{$key};
}

1;
