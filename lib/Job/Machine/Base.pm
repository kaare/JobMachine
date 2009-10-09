package Job::Machine::Base;

=head1 NAME

Job::Machine::Base -Base class for Job Classes

=cut

use strict;
use warnings;
use Net::Stomp;

=head1 METHODS

=head2 new

  my $client = Job::Machine::Base->new(
      hostname => 'localhost',
      port     => 61613,
      username => 'user',
      password => 'password',
      jobclass => 'queue.subqueue',
  );

Arguments:

hostname and port points to your Stomp message server.

username, password are just passed to the server. May not be used, depending on the server

jobclass is the channel to the worker.

=cut

sub new {
    my ($class, %args) = @_;

    $args{queue} = '/queue/' . (delete $args{jobclass} || 'subqueue');

    my $config = {
        hostname => 'localhost',
        port     => 61613,
        username => 'user',
        password => 'password',
        %args
    };

    my $stomp  = Net::Stomp->new(
        {   hostname => $config->{hostname},
            port     => $config->{port},
        }
    );

    $stomp->connect( { login => $config->{username}, passcode => $config->{password} } );

    return bless {
        config => $config,
        stomp  => $stomp,
    }, $class;
}

sub id {
    my ( $self, $id ) = @_;
    $self->{id} = $id if defined $id;
    return $self->{id};
}

sub subscribe {
    my ( $self, $queue ) = @_;
    my $stomp = $self->{stomp};
    $stomp->subscribe(
        {   destination => $queue || $self->{config}{queue},
            ack => 'client'
        }
    );
}

sub DESTROY {
    my $self  = shift;
    my $stomp = $self->{stomp};
    $stomp->disconnect() if defined $stomp;
}

=head1 AUTHOR

Kaare Rasmussen <kaare@cpan.org>.

=head1 COPYRIGHT

Copyright (C) 2009, Kaare Rasmussen

This module is free software; you can redistribute it or modify it
under the same terms as Perl itself.

=cut

1;
