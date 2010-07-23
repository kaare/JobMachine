#!perl

# Flow:

# Rely on client.t having sent a message (could do it ourself in startup)
# Send a new message and set up listening
# Reply to that message
# Check the answer

use strict;
use warnings;
use Test::More tests => 8;

my %config = (dsn => 'dbi:Pg:dbname=test', queue => 'qyouw',);
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

sub timeout {5}

sub startup {
	my ($self) = @_;
	my %config = (dsn => 'dbi:Pg:dbname=test', queue => 'qyouw',);
	ok(my $client = Job::Machine::Client->new(%config),'New client');
	$self->{client} = $client;
	ok($id = $client->send({data => 'Try Our Tasty Foobar!'}),'Send a task');
}

sub process {
	my ($self, $data) = @_;
	my $client = $self->{client};
	is(my $res = $client->check($id),undef,'Check for no message');
	ok($self->reply({data => "You've got nail"}), 'Talking to ourself');
	ok($res = $client->receive($id),'- But do we listen?');
	ok($res = $client->uncheck($id),'Uncheck first message');
	exit;
};
