package Job::Machine::Worker;

use strict;
use warnings;

use base 'Job::Machine::Base';

sub reply {
	my ($self,$data,$queue) = @_;
	my $db = $self->db;
	$queue ||= $self->{queue};
	$self->result($data,$queue);
	my $task_id = $db->task_id;
## Payload: Status of result, result id...
	$db->notify(queue => $task_id, reply => 1);
	return $task_id;
}

sub result {
	my ($self,$data,$queue) = @_;
	$queue ||= $self->{queue};
	$self->db->insert_result($data,$queue);
	$self->db->set_task_status(200);
}

sub receive {
	my $self = shift;
	$self->startup;
	my $db = $self->{db};
	$self->subscribe($self->{queue});
	$self->_check_queue($self->{queue});
	while ($self->keep_running && (my $notifies = $db->set_listen($self->timeout))) {
		my ($queue,$pid) = @$notifies;
		$self->_do_chores() && next unless $queue;

		$self->_check_queue($self->{queue});
	}
	return;
};

sub _check_queue {
	my $self = shift;
	my $db = $self->{db};
	while (my $task = $self->db->fetch_work_task($self->{queue})) {
		## log process call
		$self->process($task);
	}
}

sub _do_chores {
	my $self = shift;
	my $db = $self->{db};
	my @chores = (
		sub {
			my $self = shift;
			my $number = $db->revive_tasks($self->max_runtime) || 0;
			$self->log("Revived tasks: $number");
		},
		sub {
			my $self = shift;
			my $number = $db->fail_tasks($self->retries) || 0;
			$self->log("Failed tasks: $number");
		},
		sub {
			my $self = shift;
			my $number = $db->remove_tasks($self->remove_after) || 0;
			$self->log("Removed tasks: $number");
		},
	);
	my $chore = $chores[int(rand(@chores))];
	$self->$chore;
}

sub startup {}

sub process {die 'Subclasss me!'}

sub max_runtime {return 30*60}

sub timeout {return 300}

sub retries {return 3}

sub remove_after {return 30}

sub keep_running {return 1}

1;
__END__
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

=head3 startup

 startup will be called before any tasks are fetched and any processing is done.

 Call this method for one-time initializing.

=head3 process

 Subclassable process method.

 E.g. 

 sub process {
	my ($self, $data) = @_;
	... process $data 
	$self->reply({answer => 'Something'});
 };

=head3 max_runtime

If the default of 30 minutes isn't suitable, return the number of seconds a
process is expected to run.

A task will not be killed if it runs for longer than max_runtime. This setting
is only used when reviving tasks that are suspected to be dead.

=head3 timeout

If the default of 5 minutes isn't suitable, return the number of seconds the
worker should wait for inout before doing housekeeping chores.

If you don't want the worker to perform any housekeeping tasks, return undef

=head3 retries

If the default of 3 times isn't suitable, return the number of times a task is
retried before failing.

=head3 remove_after

If the default of 30 days isn't suitable, return the number of days a task will
remain in the database before being removed.

Return 0 if you never want tasks to be removed.


=head3 keep_running

Worker will wait for next message if this method returns true.

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
  
  receive subscribes the worker to the queue and waits for a message to be passed along.
  It will first see if there are any messages to be processed.

=head1 SEE ALSO

L<Job::Machine::Base>.

=head1 AUTHOR

Kaare Rasmussen <kaare@cpan.org>.

=head1 COPYRIGHT

Copyright (C) 2009-2010, Kaare Rasmussen

This module is free software; you can redistribute it or modify it
under the same terms as Perl itself.

=cut
