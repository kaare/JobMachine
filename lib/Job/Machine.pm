package Job::Machine;

1;
__END__
=pod

=head1 NAME

Job::Machine - Job queue handling

=head1 SYNOPSIS

The Client:

  my $client = Job::Machine::Client->new(queue => 'job.task');
  my $id = $client->send({foo => 'bar'});

The Worker is a subclass

  use base 'Job::Machine::Worker';

  sub process {
      my ($self, $data) = @_;
      $self->reply({baz => 'Yeah!'}) if $data->{foo} eq 'bar';
  };

and then use the worker

  my $worker = Worker->new(queue => 'job.task');
  $worker->receive;

Back at the Client:

  if ($client->check('reply')) {
      print $client->receive->{baz};
  }

=head1 DESCRIPTION

A small, but versatile system for sending jobs to a message queue and, if necessary,
communicating answers back to the sender.

Job::Machine uses LISTEN / NOTIFY from PostgreSQL to send signals between
clients and workers. This ensures very efficient message passing, giving any
worker that is awake the chance to start working immediately.

=head2 Database Connection

Both client and worker accepts a Database Handle (dbh), or a Data Source Name (dsn).

From scratch:

  my $client = Job::Machine::Client->new(
    dsn => 'dbi:Pg:dbname=jobqueue',
    queue => 'my.queue',
  );

Hot Handle:

  my $dbh = $self->existing_dbh;
  my $client = Job::Machine::Client->new(
    dbh => $dbh,
    queue => 'my.queue',
  );

=head2 Queue

Normally the queue name is passed as a parameter to new, but it can be overriden
for any method call.

The queue can be named anything PostgreSQL accepts. A good idea is to maintain a
hierarchical structure. e.g. I<gl.accounting> or I<message.email>.

=head1 SUPPORT

Report tickets to http://rt.cpan.org/Job-Machine/

=head1 AUTHOR

Kaare Rasmussen <kaare@cpan.org>.

=head1 COPYRIGHT

Copyright (C) 2009, Kaare Rasmussen

This module is free software; you can redistribute it or modify it
under the same terms as Perl itself.
