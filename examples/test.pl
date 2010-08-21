#!/usr/bin/perl

use 5.010;
use strict;
use warnings;
use Job::Machine::Client;

our $id;

sub data  {
	return {
		message => 'Try Our Tasty Foobar!',
		number  => 1,
		array   => [1,2,'three',],
	};
};

sub config {
	return dsn => 'dbi:Pg:dbname=test', queue => 'test';
}

sub keep_running {0}

sub startup {
	my ($self) = @_;
#	my $client = Job::Machine::Client->new(config);
#	$self->{client} = $client;
#	ok($id = $client->send({data => $self->data}),'Send a task');
}

sub process {
	my ($self, $task) = @_;
	is_deeply($task->{data}, $self->data,'- Did we get what we sent?');
	my $client = $self->{client};
	is(my $res = $client->check($id),undef,'Check for no message');
	my $reply = "You've got nail";
	ok($self->reply({data => $reply}), 'Talking to ourself');
	ok($res = $client->receive($id),'- But do we listen?');
	is($res, $reply,'- Did we hear what we said?');
	ok($client->uncheck($id),'Uncheck first message');
};

sub _worker {
	my $worker = Worker->new(config);
	$worker->receive;
};

_worker;

package Worker;
use strict;
use warnings;
use base 'Job::Machine::Worker';

sub timeout {5}

sub remove_after {360}
