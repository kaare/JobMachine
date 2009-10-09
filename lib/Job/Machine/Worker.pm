package Job::Machine::Worker;

=head1 NAME

Job::Machine::Worker - Base class for Job Workers

=head1 DESCRIPTION

  Inherits from Job::Machine::Base.
  
  All you have to do to write a worker for a particular Job Class is
  
  use base 'Job::Machine::Worker';

  sub process {
      my ($self, $data) = @_;
      ... do stuff
  };

=head1 METHODS

=cut

use strict;
use warnings;

use JSON::XS;

use base 'Job::Machine::Base';

=head2 reply

  $worker->reply($some_structure);

  Reply to a message. Use from within a Worker's process method.

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

=head2 receive

  $worker->receive;
  
  Starts the Worker's receive loop.

=cut

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

=head2 process

  Subclassable process method.
  
  E.g. 
  
  sub process {
	my ($self, $data) = @_;
	... process $data 
	$self->reply({answer => 'Something'});
  };

=cut

sub process {
    my ( $self, $data ) = @_;

	die 'Sublasss me!';
}

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
