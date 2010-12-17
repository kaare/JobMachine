package Worker;

use strict;
use warnings;
use Test::More;

use base 'Job::Machine::Worker';
use Job::Machine::Client;

our $id;
sub data  {
	return {
		message => 'Try Our Tasty Foobar!',
		number  => 1,
		array   => [1,2,'three',],
	};
};
sub timeout {5}

sub keep_running {0}

sub startup {
	my ($self) = @_;
	my %config = (dsn => 'dbi:Pg:dbname=__jm::test__', queue => 'qyouw');
	ok(my $client = Job::Machine::Client->new(%config),'New client');
	$self->{client} = $client;
	ok($id = $client->send({data => $self->data}),'Send a task');
}

sub process {
	my ($self, $task) = @_;
	is_deeply($task->{data}->{data}, $self->data,'- Did we get what we sent?');
	my $client = $self->{client};
	is(my $res = $client->check($id),undef,'Check for no message');
	my $reply = "You've got nail";
	ok($self->reply({data => $reply}), 'Talking to ourself');
	ok($res = $client->receive($id),'- But do we listen?');
	is($res, $reply,'- Did we hear what we said?');
	ok($client->uncheck($id),'Uncheck first message');
};

package Test::Job::Machine;

use base qw(Test::Class);
use Test::More;

sub db_name {'__jm::test__'};

sub startup : Test(startup => 2) {
	my $self = shift;
	my $command = 'createdb -e '.db_name;
	qx{$command} || return $self->{skip} = 1;

	$command = 'psql '.db_name.'<sql/create_tables.sql';
	ok(qx{$command},'Create Job::Machine tables') or return;
	ok($self->{dbh} = DBI->connect('dbi:Pg:dbname='.db_name), 'Connect to test database') or return;
};

sub cleanup : Test(shutdown) {
	my $self = shift;
	return if $self->{skip};

	$self->{dbh}->disconnect;
	my $command = 'dropdb '.db_name;
	qx{$command};
};

sub _worker : Test(11) {
	my $self = shift;
	return if $self->{skip};

	my %config = (dsn => 'dbi:Pg:dbname='.db_name, queue => 'qyouw',);
	ok(my $worker = Worker->new(%config),'New Worker');
	isa_ok($worker,'Worker','Worker class');
	is($worker->receive,undef,'receive loop');
};

package main;

Test::Job::Machine->runtests;
