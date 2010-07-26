CREATE schema jobmachine;
SET search_path TO jobmachine;

CREATE TABLE class (
    class_id            serial PRIMARY KEY,
    name                text,
    created             timestamp NOT NULL DEFAULT now(),
    modified            timestamp NOT NULL DEFAULT now()
);

COMMENT ON TABLE class IS 'Task class';
COMMENT ON COLUMN class.class_id IS 'Unique identification';
COMMENT ON COLUMN class.name IS 'Job class name';
COMMENT ON COLUMN class.created IS 'Timestamp for row creation';
COMMENT ON COLUMN class.modified IS 'Timestamp for latest update of this row';

CREATE TABLE task (
    task_id             serial PRIMARY KEY,
    transaction_id      integer,
    class_id            integer REFERENCES class (class_id),
    grouping            text,
    title               text,
    parameters          text,
    status              integer NOT NULL,
    run_after           timestamp DEFAULT NULL,
    remove_after        timestamp DEFAULT NULL,
    created             timestamp NOT NULL DEFAULT now(),
    modified            timestamp NOT NULL DEFAULT now()
);

COMMENT ON TABLE task IS 'Tasks';
COMMENT ON COLUMN task.task_id IS 'Unique identification';
COMMENT ON COLUMN task.transaction_id IS 'If several tasks need to be executed in sequence';
COMMENT ON COLUMN task.class_id IS 'Job class to be executed';
COMMENT ON COLUMN task.grouping IS 'Optional job group. Jobs will be retrieved by group if defined';
COMMENT ON COLUMN task.title IS 'Optional job title';
COMMENT ON COLUMN task.parameters IS 'from client to the scheduled task. Serialized with ??';
COMMENT ON COLUMN task.status IS '0 - entered, 100 - processing started, 200 - processing finished, - 900 - processing finished w/ error';
COMMENT ON COLUMN task.run_after IS 'Wait until this time to run the task';
COMMENT ON COLUMN task.remove_after IS 'Wait until this time to delete the task';
COMMENT ON COLUMN task.created IS 'Timestamp for row creation';
COMMENT ON COLUMN task.modified IS 'Timestamp for latest update of this row';

CREATE TABLE dependency (
    depends             integer REFERENCES task (task_id)
                                ON DELETE CASCADE
                                ON UPDATE CASCADE,
    depended            integer REFERENCES task (task_id)
                                ON DELETE CASCADE
                                ON UPDATE CASCADE,
    created             timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY (depends,depended)
);

COMMENT ON TABLE dependency IS 'Task dependencies';
COMMENT ON COLUMN dependency.depends IS 'Task that depends on other task';
COMMENT ON COLUMN dependency.depended IS 'Task that is depended on';
COMMENT ON COLUMN dependency.created IS 'Timestamp for row creation';

CREATE TABLE result (
    result_id           serial PRIMARY KEY,
    task_id             integer REFERENCES task (task_id)
                                ON DELETE CASCADE
                                ON UPDATE CASCADE,
    result              text,
    resulttype          text,
    created             timestamp NOT NULL DEFAULT now()
);

COMMENT ON TABLE result IS 'Results';
COMMENT ON COLUMN result.result_id IS 'Unique identification';
COMMENT ON COLUMN result.task_id IS 'Task of the result';
COMMENT ON COLUMN result.result IS 'Result of the job';
COMMENT ON COLUMN result.resulttype IS 'Type of result; xml, html, etc';
COMMENT ON COLUMN result.created IS 'Timestamp for row creation';

CREATE TABLE schedule (
       schedule_id          serial PRIMARY KEY,
       title                text,
       schedule             text,
       class_id             integer REFERENCES class (class_id),
       method               text,
       parameters           text,
       updated              timestamp NOT NULL DEFAULT now()
);

COMMENT ON TABLE schedule IS 'Schedules';
COMMENT ON COLUMN schedule.schedule_id IS 'Unique identification';
COMMENT ON COLUMN schedule.title IS 'Title of the schedule';
COMMENT ON COLUMN schedule.schedule IS 'The schedule. "hh:mm"';
COMMENT ON COLUMN schedule.class_id IS 'Class of scheduled method';
COMMENT ON COLUMN schedule.parameters IS 'Parameters for the method';
COMMENT ON COLUMN schedule.updated IS 'Timestamp for latest update of this row';