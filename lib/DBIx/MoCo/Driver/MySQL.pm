package DBIx::MoCo::Driver::MySQL;
use strict;
use warnings;
use Carp;

sub insert {
    my $class  = shift;
    my $option = ref $_[-1] eq 'HASH' ? pop : {};
    my %args   = @_;

    $class->call_trigger(before_insert => \%args);

    my $sqla = $class->db->__sqla;
    my $self = $class->new(%args);
    my ($sql, @binds) = $sqla->insert($class->table, \%args);

    if (my $update_keys = $option->{on_duplicate_key_update}) {
        my %update_args = ref $update_keys eq 'HASH' ? %$update_keys : map { $_ => $args{$_} } @$update_keys;
        my ($update_sql, @update_binds) = $sqla->update('_', \%update_args);
        $update_sql =~ s/^UPDATE _ SET/ON DUPLICATE KEY UPDATE/i;
        $sql   = "$sql $update_sql";
        @binds = (@binds, @update_binds);
    }

    if ($option->{ignore}) {
        $sql =~ s/^INSERT /INSERT IGNORE /i;
    }

    if ($option->{delayed}) {
        $sql =~ s/^INSERT /INSERT DELAYED /i;
    }

    $class->db->execute($sql, undef, \@binds)
        or croak 'Could not insert';

    my $pk = $class->primary_keys->[0];
    unless (defined $args{$pk}) {
        my $id = $class->db->last_insert_id;
        $self->set($pk => $id);
    }

    $class->call_trigger(after_insert => $self);

    $self;
}

sub replace {
    my ($class, %args) = @_;

    my ($sql, @binds) = $class->db->__sqla->insert($class->table, \%args);
    $sql =~ s/^INSERT /REPLACE / or die;
    $class->db->execute($sql, undef, \@binds)
        or croak "Could not replace: $DBI::errstr";

    return undef;
}

sub update {
    my $self   = shift;
    my $option = (@_ % 2 == 1 && ref $_[-1] eq 'HASH' ? pop : {});
    my %args   = @_;

    my $where = $self->primary_keys_hash;
    return unless $where && %$where;

    $self->flush_self_cache;

    my $table = $self->table;
    $table = "LOW_PRIORITY $table" if $option->{low_priority};
    $self->db->update($table, \%args, $where);
}

1;

__END__

=head1 NAME

DBIx::MoCo::Driver::MySQL - Provides implementation of MySQL specific methods

=head1 SYNOPSIS

  use base qw(DBIx::MoCo);

  __PACKAGE__->db_object(Some_MySQL_DB_Object);

  # now you can
  __PACKAGE__->insert(
      foo => 1,
      bar => 'baz', 
      { on_duplicate_key_update => 'bar' }
  );
  # executes
  #   INSERT INTO ... (foo, bar) VALUES (1, 'baz') ON DUPLICATE KEY UPDATE bar = 'baz'

=head1 METHODS PROVIDED

=over 4

=item insert(column => $value, ..., [ \%option ])

Executes C<INSERT> SQL. Supply %option to customize query.

Possible %option's are:

=over 4

=item $option{ignore} = 1

Executes C<INSERT IGNORE> instead of C<INSERT>.

=item $option{on_duplicate_key_update} = \@columns or { column => $sqla_value }

Executes C<INSERT ... ON DUPLICATE KEY UPDATE col = ...>.

When this option is set to an array reference, which is list of columns, such as [ 'col1', 'col2' ],
this method adds C<ON DUPLICATE KEY UPDATE col1 = col1, col2 = col2> to the query executed.

When set to a hash reference, whish is SQL::Abstract->update recognizable value,
such as { col1 => \'col1 + 1' }, this method adds C<ON DUPLICATE KEY UPDATE col1 = col1 + 1>
to the query executed.

=back

=item replace(col => $value, ...)

Executes C<REPLACE> instead of C<INSERT>.

=item update(col => $value, ..., [ \%option ])

Executes C<INSERT> SQL. Supply %option to customize query.

Possible %option's are:

=over 4

=item $option{low_priority} = 1

Executes C<UPDATE LOW_PRIORITY> instead of C<UPDATE>.

=back

=back

=cut
