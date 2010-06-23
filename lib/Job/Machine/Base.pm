package Job::Machine::Base;

use strict;
use warnings;
use Job::Machine::DB;

sub new {
	my ($class, %args) = @_;
	$args{db} = Job::Machine::DB->new( %args );
	$args{timeout} ||= 300;
	return bless \%args, $class;
}

sub db { return $_[0]->{db} };

sub id {
	my ($self, $id) = @_;
	$self->{id} = $id if defined $id;
	return $self->{id};
}

sub subscribe {
	my ($self, $queue) = @_;
	$queue ||= $self->{queue};
	$self->db->listen(queue => $queue);
}

1;
__END__
=head1 NAME

Job::Machine::Base - Base class both for Client and Worker Classes

=head1 METHODS

=head2 new

  my $client = Job::Machine::Base->new(
	  dbh   => $dbh,
	  jobclass => 'queue.subqueue',

  );

  my $client = Job::Machine::Base->new(
	  dsn   => @dsn,
  );

Arguments:

Either provide an already warm database handle, or give a new array to tell how
to open a database.

 jobclass is the channel to the worker.
 timeout is how long to wait for notifications before doing a housekeeping loop.
 Default is 5 minutes.

=head1 AUTHOR

Kaare Rasmussen <kaare@cpan.org>.

=head1 COPYRIGHT

Copyright (C) 2010, Kaare Rasmussen

This module is free software; you can redistribute it or modify it
under the same terms as Perl itself.

=cut