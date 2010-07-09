package Job::Machine::Client;

use strict;
use warnings;

use base 'Job::Machine::Base';

sub send {
	my ($self, $data, $queue) = @_;
	$queue ||= $self->{queue};
	$queue = Job::Machine::Base::QUEUE_PREFIX . $queue;
	my $id = $self->db->insert_task($data,$queue);
	$self->{db}->notify(queue => $queue);
	return $id;
}

sub check {
	my ($self, $id) = @_;
	$id ||= $self->id;
	my $queue = Job::Machine::Base::RESPONSE_PREFIX . $id;
	$self->subscribe($queue);
	return $self->db->get_notification;
}

sub receive {
	my ($self, $id) = @_;
	$id ||= $self->id;
	return $self->db->fetch_result($id);
};

1;
__END__
=head1 NAME

Job::Machine::Client - Class for Job Clients

=head1 METHODS

=head2 send

 Send a message to the configured queue

NB TODO columns to set TODO NB
    class_id is the queue name
    grouping some kind of coaleshing value (for some kind of get_next method in process)
    title ??
    parameters could be json, is that in Pg 9.0?
    run_after default to now, but possible to set somehow
    remove_after possible to set. If not set, make it modified +30 days when completed or failed
NB TODO columns to set TODO NB

 returns the message id.

=head2 check

 Check for reply. 
 
 Parameter: The message id.

=head2 receive

 Receive the reply.

 Parameter: The message id.

=head1 SEE ALSO

L<Job::Machine::Base>.

=head1 AUTHOR

Kaare Rasmussen <kaare@cpan.org>.

=head1 COPYRIGHT

Copyright (C) 2009-2010, Kaare Rasmussen

This module is free software; you can redistribute it or modify it
under the same terms as Perl itself.

=cut
