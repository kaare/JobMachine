package Job::Machine::DB;

use strict;
use warnings;
use Carp qw/croak confess/;
use DBI;
use JSON::XS;

use constant QUEUE_PREFIX    => 'jm:';
use constant RESPONSE_PREFIX => 'jmr:';

sub new {
    my ($class, %args) = @_;
    croak "No connect information" unless $args{dbh} or $args{dsn};

	$args{dbh}    ||= DBI->connect($args{dsn});
	$args{schema} ||= 'jobmachine';
    return bless \%args, $class;
}

sub listen {
    my ($self, %args) = @_;
    my $queue = $args{queue} || return undef;

    my $prefix = $args{reply} ?  RESPONSE_PREFIX :  QUEUE_PREFIX;
	$queue = $prefix . $queue;
	$self->{dbh}->do(qq{listen "$queue";});
}

sub unlisten {
    my ($self, %args) = @_;
    my $queue = $args{queue} || return undef;

    my $prefix = $args{reply} ?  RESPONSE_PREFIX :  QUEUE_PREFIX;
	$queue = $prefix . $queue;
	$self->{dbh}->do(qq{unlisten "$queue";});
}

sub notify {
    my ($self, %args) = @_;
    my $queue = $args{queue} || return undef;

    my $prefix = $args{reply} ?  RESPONSE_PREFIX :  QUEUE_PREFIX;
	$queue = $prefix . $queue;
	$self->{dbh}->do(qq{notify "$queue";});
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
	my ($self,$queue,$pid) = @_;
	$self->{current_table} = 'task';
	my $sql = qq{
		UPDATE
			"$self->{schema}".$self->{current_table} t
		SET
			status=?,
			modified=default
		FROM
			"$self->{schema}".class c
		WHERE
			status=? AND t.class_id=c.class_id AND c.name=?
			AND run_after IS NULL or run_after > now()
		RETURNING
			*
	};
	my $startstatus = 0; # read this
	my $endstatus = 100; # set to this
	my $task = $self->select_first(
		sql => $sql,
		data => [$endstatus,$startstatus,$queue]
	) || return;

	$self->{task_id} = $task->{task_id};
	$task->{data} = decode_json( delete $task->{parameters} )->{data};
	return $task;
}

sub insert_task {
	my ($self,$data,$queue) = @_;
	my $class = $self->fetch_class($queue);
	$self->{current_table} = 'task';
	my $frozen = encode_json($data);
	my $sql = qq{
		INSERT INTO
			"$self->{schema}".$self->{current_table}
			(class_id,parameters,status)
		VALUES
			(?,?,?)
	};
	$self->insert(sql => $sql,data => [$class->{class_id},$frozen,0]);
}

sub set_task_status {
	my ($self,$status) = @_;
	my $id = $self->task_id;
	$self->{current_table} = 'task';
	my $sql = qq{
		UPDATE
			"$self->{schema}".$self->{current_table}
		SET
			status=?
		WHERE 
			task_id=?
	};
	$self->update(sql => $sql,data => [$status,$id]);
}

sub fetch_class {
	my ($self,$queue) = @_;
	$self->{current_table} = 'class';
	my $sql = qq{SELECT * FROM "$self->{schema}".$self->{current_table} WHERE name=?};
	return $self->select_first(sql => $sql,data => [$queue]) || $self->insert_class($queue);
}

sub insert_class {
	my ($self,$queue) = @_;
	my $sql = qq{INSERT INTO "$self->{schema}".$self->{current_table} (name) VALUES (?) RETURNING *};
	$self->select_first(sql => $sql,data => [$queue]);
}

sub insert_result {
	my ($self,$data,$queue) = @_;
	$self->{current_table} = 'result';
	my $frozen = encode_json($data);
	my $sql = qq{
		INSERT INTO
			"$self->{schema}".$self->{current_table}
			(task_id,result)
		VALUES
			(?,?)
	};
	$self->insert(sql => $sql,data => [$self->{task_id},$frozen]);
}

sub fetch_result {
	my ($self,$id) = @_;
	$self->{current_table} = 'result';
	my $sql = qq{SELECT * FROM "$self->{schema}".$self->{current_table} WHERE task_id=? ORDER BY result_id DESC};
	my $result = $self->select_first(sql => $sql,data => [$id]) || return;

	return decode_json($result->{result})->{data};

}

# 1. Find started tasks that have passed the time limit, most probably because 
# of a dead worker. (status 100, modified < now - max_runtime)
# - set max_runtime to something reasonable, default 30 minutes but user settable
# Write a null result (?)
# - Trim status so we can try again
sub revive_tasks {
	my ($self) = @_;
	$self->{current_table} = 'task';
	my $status = 100;
	my $max = 1800;
	my $sql = qq{SELECT * FROM "$self->{schema}".$self->{current_table} WHERE status=? AND modified < now() - INTERVAL '$max seconds'};
	my $result = $self->select_first(sql => $sql,data => [$status]) || return;
use Data::Dumper;
print STDERR Dumper $result;
}

sub select_first {
    my ($self, %args) = @_;
	my $sth = ($args{sth}) ? $args{sth} : $self->dbh->prepare($args{sql}) || return 0;
	unless($sth->execute(@{$args{data}})) {
		my @c = caller;
		print STDERR "File: $c[1] line $c[2]\n";
		print STDERR $args{sql}."\n" if($args{sql});
		return 0;
	}
	my $r = $sth->fetchrow_hashref();
	$sth->finish();
	return( $r );
}

sub do {
	my ($self, %args) = @_;
	my $sth;
	if ($args{sth}) {
		$sth = $args{sth};
	} elsif ($args{sql}) {
		$sth = $self->dbh->prepare($args{sql});
	} else {
		$sth = $self->{last_sth} || return undef;
	}
	$self->{last_sth} = $sth;
	return $sth->execute(@{$args{data}});
}

sub prepare {
	my ($self, %args) = @_;
	my $sth = $self->dbh->prepare($args{sql} || return undef) || return undef;

	$self->{last_sth} = $sth;
	return $sth;
}

sub insert {
	my $self = shift;
	$self->do(@_);
	return $self->dbh->last_insert_id(undef,$self->{schema},$self->{current_table},undef);
}

sub update {
	my $self = shift;
	$self->do(@_);
}

sub dbh {
	return $_[0]->{dbh} || confess "No database handle";
}

sub task_id {
	return $_[0]->{task_id} || confess "No task id";
}
sub disconnect {
	return $_[0]->{dbh}->disconnect;
}

sub DESTROY {
    $_[0]->disconnect();
}

1;
__END__

=head1 NAME

Job::Machine::DB - Database class for Job::Machine

=head1 METHODS

=head2 new

  my $client = Job::Machine::DB->new(
      dbh   => $dbh,
      jobclass => 'queue.subqueue',

  );

  my $client = Job::Machine::Base->new(
      dsn   => @dsn,
  );


=head2 set_listen

Sets up the listener

=cut
