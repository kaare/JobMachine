package Job::Machine::Worker;

use strict;
use warnings;

use base 'Job::Machine::Base';

sub reply {
	my ($self,$data,$queue) = @_;
	my $db = $self->db;
	$queue ||= $self->{queue};
	$queue = Job::Machine::Base::QUEUE_PREFIX . $queue;
	$self->result($data,$queue);
	my $task_id = $db->task_id;
## Payload: Status of result, result id...
	$queue = Job::Machine::Base::RESPONSE_PREFIX . $task_id;
	$db->notify(queue => $queue);
}

sub result {
	my ($self,$data,$queue) = @_;
	$queue ||= $self->{queue};
	$self->db->insert_result($data,$queue);
}

sub receive {
	my $self = shift;
	my $db = $self->{db};
	$self->subscribe;
	while (my $notifies = $db->set_listen($self->timeout)) {
		my ($queue,$pid) = @$notifies;
		$self->do_chores() && next unless $queue;

		my $task = $self->db->fetch_work_task($queue,$pid);
## log process call
		$self->process($task);
	}
};

sub do_chores {
	my $self = shift;
	my $db = $self->{db};
	my @chores = (
		sub {
			my $self = shift;
			$self->log('tjore 1');
			# 1. Find started tasks that have passed the time limit, most probably because 
			# of a dead worker. (status 100, modified < now - max_runtime)
			# - set max_runtime to something reasonable, default 30 minutes but user settable
			# Write a null result (?)
			# - Trim status so we can try again
		},
		sub {
			my $self = shift;
			$self->log('tjore 2');
			# 2. Find tasks that have failed too many times (# of result rows > max limit
			# - fail them (Set status 900)
			# - log
		},
		sub {
			my $self = shift;
			$self->log('tjore 3');
			# 3. Find tasks that should be removed (remove_task > now)
			# - delete them
			# - log
		},
	);
	my $chore = $chores[int(rand(@chores))];
	$self->$chore;
}

sub process {die 'Subclasss me!'}

sub max_runtime {return 30*60}

sub timeout {return 300}

sub retries {return 3}

=head1 NAME

Job::Machine::Worker - Base class for Job Workers

=head1 DESCRIPTION

  Inherits from Job::Machine::Base.
  
  All you have to do to write a worker for a particular Job Class is
  
  use base 'Job::Machine::Worker';

  sub process {
	  my ($self, $task) = @_;
	  ... do stuff
  };

=head1 METHODS

=head2 Methods to be subclassed

A worker process always needs to subclass the process method with the
real functionality.

=head3 process

 Subclassable process method.

 E.g. 

 sub process {
	my ($self, $data) = @_;
	... process $data 
	$self->reply({answer => 'Something'});
 };

=head3 max_runtime

If the default of 30 minutes isn't suitable, make this method return the
number of seconds a process is allowed to run.

=head3 timeout

If the default of 5 minutes isn't suitable, make this method return the
number of seconds the worker should wait for inout before doing housekeeping
chores.

=head3 retries

If the default of 3 times isn't suitable, make this method return the
number of times a task is retried before failing.

=head2 Methods to be used from within the process method

=head3 reply

  $worker->reply($some_structure);

  Reply to a message. Use from within a Worker's process method.

=head3 result

  $worker->result($result_data);

  Save the result of the task. Use from within a Worker's process method.

=head3 db

 Get the DB class. From this it's possible to get the database handle
 
 my $dbh = $self->db->dbh;
 
 If you use the same database for Job::Machine as for your other data, this
 handle can be used by your worker module.

=head3 id

=head2 methods not to be disturbed

=head3 receive

  $worker->receive;
  
  Starts the Worker's receive loop.

=head1 SEE ALSO

L<Job::Machine::Base>.

=head1 AUTHOR

Kaare Rasmussen <kaare@cpan.org>.

=head1 COPYRIGHT

Copyright (C) 2009-2010, Kaare Rasmussen

This module is free software; you can redistribute it or modify it
under the same terms as Perl itself.

=cut

1;
