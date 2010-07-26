#!perl

# Flow:

# Rely on client.t having sent a message (could do it ourself in startup)
# Send a new message and set up listening
# Reply to that message
# Check the answer

use strict;
use warnings;
use Test::More;

eval "use DBD::Pg";
if ($@) {
	plan skip_all => "DBD::Pg required for testing Job::Machine::Client";
} else {
	plan tests => 10;
}

my %config = (dsn => 'dbi:Pg:dbname=__jm::test__', queue => 'qyouw',);
ok(my $worker = Worker->new(%config),'New Worker');
isa_ok($worker,'Worker','Worker class');
ok($worker->receive,'receive loop');

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

sub startup {
	my ($self) = @_;
	my %config = (dsn => 'dbi:Pg:dbname=__jm::test__', queue => 'qyouw',);
	ok(my $client = Job::Machine::Client->new(%config),'New client');
	$self->{client} = $client;
	ok($id = $client->send({data => $self->data}),'Send a task');
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
	exit;
};
