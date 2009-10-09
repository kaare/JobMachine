package Job::Machine::Client;

=head1 NAME

Job::Machine::Client - Class for Job Clients

=head1 METHODS

=cut

use strict;
use warnings;
use JSON::XS;

use base 'Job::Machine::Base';

=head2 send

Send a message to the configured queue

=cut

sub send {
    my ( $self, $data ) = @_;
    my $stomp  = $self->{stomp};
    my $frozen = encode_json({ id => $self->id, data => $data, });
    $stomp->send(
        {   destination => $self->{config}{queue},
            body        => $frozen,
            persistent  => 'true',
        }
    );
}

=head2 check

Check for reply. Remember to use same id as when the initial message was sent.

=cut

sub check {
    my ($self) = @_;
    my $queue = $self->{config}{queue} . '/' . $self->id;
    $self->subscribe($queue);
    my $stomp    = $self->{stomp};
    my $can_read = $stomp->can_read({
        timeout     => '0.1',
        destination => $queue,
    });
}

=head2 receive

Receive the reply. Remember to use same id as when the initial message was sent.

=cut

sub receive {
    my ( $self ) = @_;

    my $queue = $self->{config}{queue} . '/' . $self->id;
	my $stomp = $self->{stomp};
    my $frame = $stomp->receive_frame;
    my $thawed = decode_json( $frame->body );
    $stomp->ack( { frame => $frame } );
    $stomp->disconnect();
	return $thawed->{data};
};

=head1 SEE ALSO

L<Job::Machine::Base>.

=head1 AUTHOR

Kaare Rasmussen <kaare@cpan.org>.

=head1 COPYRIGHT

Copyright (C) 2009, Kaare Rasmussen

This module is free software; you can redistribute it or modify it
under the same terms as Perl itself.

=cut

1;
