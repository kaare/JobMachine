package Job::Machine::Worker;

use strict;
use warnings;

use base 'Job::Machine::Base';

sub reply {
	my ($self,$data,$queue) = @_;
	$queue ||= $self->{queue};
	$queue .= '.' . $self->id;
	$self->result($data,$queue);
	$self->db->notify(queue => $queue);
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

	while (my $notifies = $db->set_listen) {
		my ($queue,$pid) = @$notifies;
		next unless $queue;

		my $task = $self->db->fetch_task($queue,$pid);
		$self->process($task);
	}
};

sub process {die 'Subclasss me!'}

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

=head2 reply

  $worker->reply($some_structure);

  Reply to a message. Use from within a Worker's process method.


=head2 result

  $worker->result($result_data);

  Save the result of the task. Use from within a Worker's process method.

=head2 receive

  $worker->receive;
  
  Starts the Worker's receive loop.

=head2 process

  Subclassable process method.
  
  E.g. 
  
  sub process {
	my ($self, $data) = @_;
	... process $data 
	$self->reply({answer => 'Something'});
  };

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
