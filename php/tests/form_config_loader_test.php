<?php

$configPath = tempnam(sys_get_temp_dir(), 'crm-form-config-');
if ($configPath === false) {
	fwrite(STDERR, "FAIL: Could not create the temporary CRM form configuration.\n");
	exit(1);
}

$configSource = <<<'PHP'
<?php
return array(
	'CRM_WEBHOOK_URL' => 'https://leadops-console.onrender.com/webhooks/form/3d-preciscan',
	'CRM_WEBHOOK_SECRET' => 'file-backed-test-secret',
	'NOT_ALLOWED' => 'must-not-load',
);
PHP;
file_put_contents($configPath, $configSource);
define('CRM_FORM_CONFIG_PATH', $configPath);
require_once dirname(__DIR__) . '/public_html/php/fonctions.php';

$failures = array();
function config_test_expect($condition, $message)
{
	global $failures;
	if (!$condition) $failures[] = $message;
}

putenv('CRM_WEBHOOK_URL');
putenv('CRM_WEBHOOK_SECRET');
config_test_expect(
	crm_form_config_path() === $configPath,
	'The explicit test path must be used for the server-only form configuration.'
);
config_test_expect(
	crm_environment_value('CRM_WEBHOOK_URL') === 'https://leadops-console.onrender.com/webhooks/form/3d-preciscan',
	'The webhook URL must load from the server-only configuration file.'
);
config_test_expect(
	crm_environment_value('CRM_WEBHOOK_SECRET') === 'file-backed-test-secret',
	'The webhook secret must load from the server-only configuration file.'
);
config_test_expect(
	crm_environment_value('NOT_ALLOWED') === '',
	'Unknown configuration keys must not be exposed to the form runtime.'
);

putenv('CRM_WEBHOOK_SECRET=stale-environment-secret');
config_test_expect(
	crm_environment_value('CRM_WEBHOOK_SECRET') === 'file-backed-test-secret',
	'The protected file value must override a stale web-server environment variable.'
);

@unlink($configPath);
putenv('CRM_WEBHOOK_SECRET');

if (!empty($failures)) {
	foreach ($failures as $failure) fwrite(STDERR, 'FAIL: ' . $failure . PHP_EOL);
	exit(1);
}

echo "PHP server-only form configuration loader tests passed.\n";
