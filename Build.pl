#!perl

use Module::Build;

my $build = Module::Build->new(
    dist_version_from => 'lib/Job/Machine.pod',
    module_name => 'Job::Machine',
    license     => 'perl',
    requires    => {
        'perl'       => '5.8.3',
        'Net::Stomp' => '0.34',
        'JSON::XS'   => '2.1',
    },
);
$build->create_build_script;
