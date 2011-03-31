use strict;
use warnings;
no  warnings 'once';
use File::Spec;
use lib File::Spec->catdir('t', 'lib');
use Test::More;
use Test::Exception;
use Blog::Entry;

unless (eval { require Test::mysqld }) {
    plan skip_all => 'Could not load Test::mysqld'
}

my $mysqld = Test::mysqld->new(
    my_cnf => {
        'skip-networking' => '',
    }
) or plan skip_all => $Test::mysqld::errstr;
t::mysql::MoCo->db->dsn($mysqld->dsn(dbname => 'test'));
t::mysql::MoCo->db->execute(<<__SQL__);
CREATE TABLE foo (
    id INT PRIMARY KEY,
    value VARCHAR(255)
)
__SQL__

plan tests => 16;

ok t::mysql::MoCo::Foo->create(
    id => 1,
    value => 'foo',
);

lives_ok {
    t::mysql::MoCo::Foo->insert(
        id => 2,
        value => 'bar',
    );
};

is t::mysql::MoCo::Foo->count, 2;

lives_ok {
    t::mysql::MoCo::Foo->insert(
        id => 1,
        value => 'foo',
        { ignore => 1 },
    );
} '{ ignore => 1 }';

is t::mysql::MoCo::Foo->count, 2, 'INSERT IGNORE';

lives_ok {
    t::mysql::MoCo::Foo->insert(
        id => 1,
        value => 'will not be inserted',
        { delayed => 1 },
    );
} '{ daleyd => 1 }';

isnt t::mysql::MoCo::Foo->find(id => 1)->value, 'will not be inserted',
     'INSERT DELAYED always succeeds';

lives_ok {
    t::mysql::MoCo::Foo->insert(
        id => 2,
        value => 'baz',
        { on_duplicate_key_update => [ 'value' ] },
    );
} '{ on_duplicate_key_update => [ ... ] }';

is t::mysql::MoCo::Foo->count, 2, 'INSERT ON DUPLICATE KEY UPDATE';

is_deeply t::mysql::MoCo::Foo->search(order => 'id')->map(sub { $_->value })->to_a,
          [ 'foo', 'baz' ];

lives_ok {
    t::mysql::MoCo::Foo->replace(
        id => 2,
        value => 'barbaz',
    );
} 'replace';

is_deeply t::mysql::MoCo::Foo->search(order => 'id')->map(sub { $_->value })->to_a,
          [ 'foo', 'barbaz' ];

my $record = t::mysql::MoCo::Foo->find(id => 2);

lives_ok {
    $record->update(value => \q< CONCAT('foo', value) >);
} 'update';

is_deeply t::mysql::MoCo::Foo->search(order => 'id')->map(sub { $_->value })->to_a,
          [ 'foo', 'foobarbaz' ];

throws_ok {
    Blog::Entry->replace;
} qr/Could not load driver DBIx::MoCo::Driver::SQLite/, 'SQLite driver is not implemented';

our $fake_time;
BEGIN {
    *CORE::GLOBAL::time = sub { $fake_time }
}

subtest current_timestamp => sub {
    my $real_time = CORE::time(); # == database's NOW()

    $fake_time = $real_time;
    is t::mysql::MoCo::DataBase->current_timestamp, $real_time;
    is t::mysql::MoCo::DataBase->current_timestamp, $real_time;
    is t::mysql::MoCo::DataBase->current_timestamp(update => 1), $real_time;
    is t::mysql::MoCo::DataBase->current_timestamp, $real_time;

    $fake_time = 1;
    is t::mysql::MoCo::DataBase->current_timestamp(update => 1), $real_time;
    is t::mysql::MoCo::DataBase->current_timestamp, $real_time;
    is t::mysql::MoCo::DataBase->current_timestamp, $real_time;
};

done_testing;

package t::mysql::MoCo;
use base 'DBIx::MoCo';

BEGIN {
    __PACKAGE__->db_object('t::mysql::MoCo::DataBase');
}

package t::mysql::MoCo::DataBase;
use base 'DBIx::MoCo::DataBase';

package t::mysql::MoCo::Foo;
use base 't::mysql::MoCo';

BEGIN {
    __PACKAGE__->table('foo');
}
