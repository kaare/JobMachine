package Job::Machine::Client;

=head2 Job::Machine::Client

Base class for Job Clients

=cut

use strict;
use warnings;
use Net::Stomp;
use JSON::XS;

use base 'Job::Machine::Base';

=pod send

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

=pod check

Check for reply

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

1;
