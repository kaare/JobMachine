#!perl

use Module::Build;

my $build = Module::Build->new(
    dist_version_from => 'lib/Job/Machine.pm',
    module_name => 'Job::Machine',
    license     => 'perl',
    requires    => {
        'perl'       => '5.8.3',
        'DBD::Pg'    => '2.0',
        'JSON::XS'   => '2.25',
    },
    add_to_cleanup      => [ 'Job-Machine-*' ],
    create_makefile_pl => 'traditional',
);
$build->create_build_script;
