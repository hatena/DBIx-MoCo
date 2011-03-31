package DBIx::MoCo::Fixture::Mock;
use strict;
use warnings;
use Tie::Hash;
use base qw/Tie::StdHash/;

sub new {
    my $class = shift;
    my $config = shift;
    my $self = bless {}, $class;
    tie %$self, 'DBIx::MoCo::Fixture::Mock';
    $self->{__config} = $config;
    $self->{__cache} = {};
    return $self;
}

sub FETCH {
    my ($self, $name) = @_;
    if ($name =~ /^(__config|__cache)$/) {
        return $self->{$name};
    }

    return $self->__fetch($name);
}

sub __fetch {
    my ($self, $name) = @_;
    unless (defined $self->{__cache}->{$name}) {
        my $config = $self->{__config};
        my $record = $config->{records}->{$name};
        my $model = $config->{model};

        my %query;
        for (@{ $model->columns }) {
            $query{$_} = $record->{$_} if exists $record->{$_};
        }
        $self->{__cache}->{$name} = $model->retrieve(%query);
        defined $self->{__cache}->{$name} or die "fixture $name key not found."
    }

    return $self->{__cache}->{$name};
}

sub DESTROY {}

sub AUTOLOAD {
    my $self = shift; 
    my $name = our $AUTOLOAD; 
    $name =~ s/.*:://o; 

    if ($name eq 'TIEHASH') {
        return;
    }

    return $self->__fetch($name);
}

1;
