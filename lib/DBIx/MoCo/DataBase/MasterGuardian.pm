package DBIx::MoCo::DataBase::MasterGuardian;
use strict;
use warnings;


sub new {
    my ($class, $db) = @_;
    $db->use_master;
    bless { db => $db }, $class;
}

sub DESTROY {
    my ($self) = @_;
    $self->{db}->no_use_master;
}


1;
