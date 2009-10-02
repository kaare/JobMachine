package Job::Machine::Base;

=head2 Job::Machine::base

Base class for Job Classes

=cut

use strict;
use warnings;
use Net::Stomp;

sub new {
    my ($class, %args) = @_;

	my $config = {
		hostname => 'localhost',
		port     => 61613,
		username => 'user',
		password => 'password',
		queue    => '/queue/sub',
		%args
	};

    my $stomp  = Net::Stomp->new(
        {   hostname => $config->{hostname},
            port     => $config->{port},
        }
    );

    my $username = $config->{username};
    my $password = $config->{password};
    $stomp->connect( { login => $username, passcode => $password } );

    return bless {
        config => $config,
        stomp  => $stomp,
    }, $class;
}

=pod id

Subclass the id generator to return the 'reply' id

=cut

sub id {
    return 1;
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

1;
