package Job::Machine::Worker;

=head2 Job::Machine::Worker

Base class for Job Workers

=cut

use strict;
use warnings;

use JSON::XS;

use base 'Job::Machine::Base';

=pod reply

=cut

sub reply {
    my ( $self, $data ) = @_;
    my $queue = $self->{config}{queue} . '/' . $self->id;
    my $frozen = encode_json($data);
    my $stomp = $self->{stomp};
    $stomp->send(
        {   destination => $queue,
            body        => $frozen,
            persistent  => 'true',
        }
    );
}

sub receive {
    my ( $self ) = @_;
	$self->subscribe;
	my $stomp = $self->{stomp};
    while ( my $frame = $stomp->receive_frame ) {
        my $thawed = decode_json( $frame->body );
        $self->{id} = $thawed->{id};
        $self->process($thawed->{data});
        $stomp->ack( { frame => $frame } );
    }

    $stomp->disconnect();
};

=pod process

Subclassable process method

=cut

sub process {
    my ( $self, $data ) = @_;

	die 'Sublasss me!';
}

1;
