package Job::Machine::DB;

use strict;
use warnings;
use Carp qw/croak confess/;
use DBI;
use Data::Serializer;

use constant QUEUE_PREFIX    => 'jm:';
use constant RESPONSE_PREFIX => 'jmr:';

sub new {
	my ($class, %args) = @_;
	croak "No connect information" unless $args{dbh} or $args{dsn};
	croak "invalid queue" if ref $args{queue} and ref $args{queue} ne 'ARRAY';

	$args{dbh_inherited} = 1 if $args{dbh};
	$args{user}     ||= undef;
	$args{password} ||= undef;
	$args{db_attr}  ||= undef;
	$args{dbh}      ||= DBI->connect($args{dsn},$args{user},$args{password},$args{db_attr});
	$args{database_schema}   ||= 'jobmachine';
	return bless \%args, $class;
}

sub serializer {
	my ($self) = @_;
	my $args = $self->{serializer_args} || {};
	$args->{serializer} ||= $self->{serializer} || 'Sereal';
	return $self->{serialize} ||= Data::Serializer->new(%$args);
}

sub listen {
	my ($self, %args) = @_;
	my $queue = $args{queue} || return undef;

	my $prefix = $args{reply} ?  RESPONSE_PREFIX :  QUEUE_PREFIX;
	for my $q (ref $queue ? @$queue : ($queue)) {
		$self->{dbh}->do(qq{listen "$prefix$q";});
	}
}

sub unlisten {
	my ($self, %args) = @_;
	my $queue = $args{queue} || return undef;

	my $prefix = $args{reply} ?  RESPONSE_PREFIX :  QUEUE_PREFIX;
	for my $q (ref $queue ? @$queue : ($queue)) {
		$self->{dbh}->do(qq{unlisten "$prefix$q";});
	}
}

sub notify {
	my ($self, %args) = @_;
	my $queue = $args{queue} || return undef;
	my $payload = $args{payload};
	my $prefix = $args{reply} ?  RESPONSE_PREFIX :  QUEUE_PREFIX;
	$queue = $prefix . $queue;
	my $sql = qq{SELECT pg_notify(?,?)};
	my $task = $self->select_first(
		sql => $sql,
		data => [ $queue, $payload],
	);
}

sub get_notification {
	my ($self,$timeout) = @_;
	my $dbh = $self->dbh;
	my $notifies = $dbh->func('pg_notifies');
	return $notifies;
}

sub set_listen {
	my ($self,$timeout) = @_;
	my $dbh = $self->dbh;
	my $notifies = $dbh->func('pg_notifies');
	if (!$notifies) {
		my $fd = $dbh->{pg_socket};
		vec(my $rfds='',$fd,1) = 1;
		my $n = select($rfds, undef, undef, $timeout);
		$notifies = $dbh->func('pg_notifies');
	}
	return $notifies || [0,0];
}

sub fetch_work_task {
	my ($self,$pid) = @_;
	my $queue = ref $self->{queue} ? $self->{queue} : [$self->{queue}];
	$self->{current_table} = 'task';
	my $elems = join(',', ('?') x @$queue);
	my $sql = qq{
		UPDATE "$self->{database_schema}".$self->{current_table} t
		SET status=100,
			modified=default
		FROM "jobmachine".class cx
		WHERE t.class_id = cx.class_id
		AND task_id = (
			SELECT min(task_id)
			FROM "$self->{database_schema}".$self->{current_table} t
			JOIN "jobmachine".class c USING (class_id)
			WHERE t.status=0
			AND c.name IN ($elems)
			AND (t.run_after IS NULL
			OR t.run_after > now())
		)
		AND t.status=0
		RETURNING *
		;
	};
	my $task = $self->select_first(
		sql => $sql,
		data => $queue
	) || return;

	$self->{task_id} = $task->{task_id};
	$task->{data} = $self->serializer->deserialize(delete $task->{parameters});
	return $task;
}

sub insert_task {
	my ($self,$data,$queue) = @_;
	my $class = $self->fetch_class($queue);
	$self->{current_table} = 'task';
	my $frozen = $self->serializer->serialize($data);
	my $sql = qq{
		INSERT INTO "$self->{database_schema}".$self->{current_table}
			(class_id,parameters,status)
		VALUES (?,?,?)
		RETURNING task_id
	};
	$self->insert(sql => $sql,data => [$class->{class_id},$frozen,0]);
}

sub set_task_status {
	my ($self,$status) = @_;
	my $id = $self->task_id;
	$self->{current_table} = 'task';
	my $sql = qq{
		UPDATE "$self->{database_schema}".$self->{current_table}
		SET status=?
		WHERE task_id=?
	};
	$self->update(sql => $sql,data => [$status,$id]);
}

sub fetch_class {
	my ($self,$queue) = @_;
	$self->{current_table} = 'class';
	my $sql = qq{
		SELECT *
		FROM "$self->{database_schema}".$self->{current_table}
		WHERE name=?
	};
	return $self->select_first(sql => $sql,data => [$queue]) || $self->insert_class($queue);
}

sub insert_class {
	my ($self,$queue) = @_;
	my $sql = qq{
		INSERT INTO "$self->{database_schema}".$self->{current_table}
			(name)
		VALUES (?)
		RETURNING class_id
	};
	$self->select_first(sql => $sql,data => [$queue]);
}

sub insert_result {
	my ($self,$data,$queue) = @_;
	$self->{current_table} = 'result';
	my $frozen = $self->serializer->serialize($data);
	my $sql = qq{
		INSERT INTO "$self->{database_schema}".$self->{current_table}
			(task_id,result)
		VALUES (?,?)
		RETURNING result_id
	};
	$self->insert(sql => $sql,data => [$self->{task_id},$frozen]);
}

sub fetch_result {
	my ($self,$id) = @_;
	$self->{current_table} = 'result';
	my $sql = qq{
		SELECT *
		FROM "$self->{database_schema}".$self->{current_table}
		WHERE task_id=?
		ORDER BY result_id DESC
	};
	my $result = $self->select_first(sql => $sql,data => [$id]) || return;

	return $self->serializer->deserialize($result->{result});
}

sub fetch_results {
	my ($self,$id) = @_;
	$self->{current_table} = 'result';
	my $sql = qq{
		SELECT *
		FROM "$self->{database_schema}".$self->{current_table}
		WHERE task_id=?
		ORDER BY result_id DESC
	};
	my $results = $self->select_all(sql => $sql,data => [$id]) || return;

	return [map { $self->serializer->deserialize($_->{result}) } @{ $results } ];
}

# 1. Find started tasks that have passed the time limit, most probably because 
# of a dead worker. (status 100, modified < now - max_runtime)
# 2. Trim status so task can be tried again

sub revive_tasks {
	my ($self,$max) = @_;
	$self->{current_table} = 'task';
	my $status = 100;
	my $sql = qq{
		UPDATE "$self->{database_schema}".$self->{current_table}
		SET status=0
		WHERE status=?
		AND modified < now() - INTERVAL '$max seconds'
	};
	my $result = $self->do(sql => $sql,data => [$status]);
	return $result;
}

# 1. Find tasks that have failed too many times (# of result rows > $self->retries
# 2. fail them (Set status 900)
# There's a hard limit (100) for how many tasks can be failed at one time for
# performance resons

sub fail_tasks {
	my ($self,$retries) = @_;
	$self->{current_table} = 'result';
	my $limit = 100;
	my $sql = qq{
		SELECT task_id
		FROM "$self->{database_schema}".$self->{current_table}
		GROUP BY task_id
		HAVING count(*)>?
		LIMIT ?
	};
	my $result = $self->select_all(sql => $sql,data => [$retries,$limit]) || return 0;
	return 0 unless @$result;

	my $task_ids = join ',',map {$_->{task_id}} @$result;
	$self->{current_table} = 'task';
	my $status = 900;
	$sql = qq{
		UPDATE "$self->{database_schema}".$self->{current_table}
		SET status=?
		WHERE task_id IN ($task_ids)
	};
	$self->do(sql => $sql,data => [$status]);
	return scalar @$result;
}

# 3. Find tasks that should be removed (remove_task < now)
# - delete them
# - log
sub remove_tasks {
	my ($self,$after) = @_;
	return 0 unless $after;

	$self->{current_table} = 'task';
	my $limit = 100;
	my $sql = qq{
		DELETE FROM "$self->{database_schema}".$self->{current_table}
		WHERE modified < now() - INTERVAL '$after days'
	};
	my $result = $self->do(sql => $sql,data => []);
	return $result;
}

sub select_first {
	my ($self, %args) = @_;
	my $sth = $self->dbh->prepare($args{sql}) || return 0;

	unless($sth->execute(@{$args{data}})) {
		my @c = caller;
		print STDERR "File: $c[1] line $c[2]\n";
		print STDERR $args{sql}."\n" if($args{sql});
		return 0;
	}
	my $r = $sth->fetchrow_hashref();
	$sth->finish();
	return ( $r );
}

sub select_all {
	my ($self, %args) = @_;
	my $sth = $self->dbh->prepare($args{sql}) || return 0;

	$self->set_bind_type($sth,$args{data} || []);
	unless($sth->execute(@{$args{data}})) {
		my @c = caller;
		print STDERR "File: $c[1] line $c[2]\n";
		print STDERR $args{sql}."\n" if($args{sql});
		return 0;
	}
	my @result;
	while( my $r = $sth->fetchrow_hashref) {
			push(@result,$r);
	}
	$sth->finish();
	return ( \@result );
}

sub set_bind_type {
	my ($self,$sth,$data) = @_;
	for my $i (0..scalar(@$data)-1) {
		next unless(ref($data->[$i]));

		$sth->bind_param($i+1, undef, $data->[$i]->[1]);
		$data->[$i] = $data->[$i]->[0];
	}
	return;
}

sub do {
	my ($self, %args) = @_;
	my $sth = $self->dbh->prepare($args{sql}) || return 0;

	$sth->execute(@{$args{data}});
	my $rows = $sth->rows;
	$sth->finish();
	return $rows;
}

sub insert {
	my ($self, %args) = @_;
	my $sth = $self->dbh->prepare($args{sql}) || return 0;

	$sth->execute(@{$args{data}});
	my $retval = $sth->fetch()->[0];
	$sth->finish();
	return $retval;
}

sub update {
	my $self = shift;
	$self->do(@_);
	return;
}

sub dbh {
	return $_[0]->{dbh} || confess "No database handle";
}

sub task_id {
	return $_[0]->{task_id} || confess "No task id";
}

sub disconnect {
	return $_[0]->{dbh}->disconnect if $_[0]->{dbh};
}

sub DESTROY {
	my $self = shift;
	$self->disconnect() unless $self->{dbh_inherited};
	return;
}

1;
__END__

=head1 NAME

Job::Machine::DB - Database class for Job::Machine

=head1 METHODS

=head2 new

  my $client = Job::Machine::DB->new(
	  dbh   => $dbh,
	  queue => 'queue.subqueue',

  );

  my $client = Job::Machine::Base->new(
	  dsn   => @dsn,
  );


=head2 set_listen

 $self->listen( queue => 'queue_name' );
 $self->listen( queue => \@queues, reply => 1  );

Sets up the listener.  Quit listening to the named queues. If 'reply' is
passed, we unlisten to the related reply queue instead of the task queue.

Return undef immediately if no queue is provided.

=head2 unlisten

 $self->unlisten( queue => 'queue_name' );
 $self->unlisten( queue => \@queues, reply => 1  );

Quit listening to the named queues. If 'reply' is passed, we unlisten
to the related reply queue instead of the task queue.

Return undef immediately if no queue is provided.

=head2 notify

 $self->notify( queue => 'queue_name' );
 $self->notify( queue => 'queue_name', reply => 1, payload => $data  );

Sends an asynchronous notification to the named queue, with an optional
payload. If 'reply' is true, then the queue names are taken to be reply.

Return undef immediately if no queue name is provided.

=head2 get_notification

 my $notifies = $self->get_notification();

Retrievies the pending notifications. The return value is an arrayref where
each row looks like this:

 my ($name, $pid, $payload) = @$notify;

=head1 AUTHOR

Kaare Rasmussen <kaare@cpan.org>.

=head1 COPYRIGHT

Copyright (C) 2009,2014, Kaare Rasmussen

This module is free software; you can redistribute it or modify it
under the same terms as Perl itself.

=cut
