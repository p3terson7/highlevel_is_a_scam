<?php

require_once dirname(__DIR__) . '/fonctions.php';

$failures = array();
$testErrorLog = tempnam(sys_get_temp_dir(), 'crm-php-test-log-');
$previousErrorLog = ini_get('error_log');
if ($testErrorLog !== false) ini_set('error_log', $testErrorLog);

putenv('CRM_FORM_PRODUCTION');
putenv('CRM_FORM_ENV=local');
putenv('TURNSTILE_SITE_KEY');
putenv('TURNSTILE_SECRET_KEY');
putenv('TURNSTILE_EXPECTED_HOSTNAMES');
putenv('CRM_RATE_LIMIT_REDIS_URL');

function test_expect($condition, $message)
{
	global $failures;
	if (!$condition) $failures[] = $message;
}

$_POST = array('value' => array('not', 'scalar'));
test_expect(crm_post_string('value', 20) === false, 'Array POST values must be rejected.');

$_POST = array('submission_id' => crm_new_submission_id());
$submissionId = crm_submission_id_from_post();
test_expect(is_string($submissionId), 'A generated submission ID must be accepted.');
test_expect(
	preg_match('/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/', $submissionId) === 1,
	'Submission IDs must be UUIDv4 values.'
);
$_GET = array('submission_id' => $submissionId);
test_expect(crm_form_submission_id() === $submissionId, 'A delivery retry must retain its submission ID.');

test_expect(crm_valid_email('client@example.com'), 'A valid email address should pass.');
test_expect(!crm_valid_email("client@example.com\r\nBcc: attacker@example.com"), 'CRLF email injection must fail.');
test_expect(!crm_valid_single_line("Subject\r\nBcc: attacker@example.com"), 'CRLF subject injection must fail.');
test_expect(crm_valid_phone('+1 (819) 313-1152'), 'A normal Canadian phone number should pass.');
test_expect(!crm_valid_phone('123-call-now'), 'Alphabetic phone values must fail.');
test_expect(crm_html('<b>"quoted"</b>') === '&lt;b&gt;&quot;quoted&quot;&lt;/b&gt;', 'HTML output must be escaped.');
test_expect(crm_safe_web_url('javascript:alert(1)') === '', 'Non-HTTP tracking URLs must fail.');
test_expect(crm_valid_webhook_url('https://crm.example.com/webhooks/form/client'), 'An HTTPS CRM webhook URL should pass.');
test_expect(!crm_valid_webhook_url('http://crm.example.com/webhooks/form/client'), 'An HTTP CRM webhook URL must fail.');
test_expect(!crm_valid_webhook_url('https://user:pass@crm.example.com/hook'), 'Webhook URL credentials must fail.');
test_expect(crm_form_error_message('not-real', 'fr') === '', 'Unknown error codes must not be rendered.');

$_POST = array();
$missingConsent = crm_sms_consent_payload('contact');
test_expect(!array_key_exists('sms', $missingConsent), 'An absent consent checkbox must be not-provided, not a withdrawal.');
test_expect($missingConsent['method'] === 'not_provided', 'Missing consent must use not_provided semantics.');
$_POST = array('sms_consent' => 'accepted');
$grantedConsent = crm_sms_consent_payload('contact');
test_expect($grantedConsent['sms'] === true, 'An explicitly checked consent box must grant consent.');
$_POST = array('sms_consent_action' => 'withdraw');
$withdrawnConsent = crm_sms_consent_payload('contact');
test_expect($withdrawnConsent['sms'] === false, 'Only an explicit withdrawal control may revoke consent.');
$_POST = array('sms_consent' => 'false');
$uncheckedConsent = crm_sms_consent_payload('contact');
test_expect(!array_key_exists('sms', $uncheckedConsent), 'A checkbox-like false value is not an explicit withdrawal.');
test_expect($uncheckedConsent['method'] === 'not_provided', 'Unchecked consent must remain not_provided.');
$_POST = array('sms_consent' => 'accepted', 'sms_consent_action' => 'withdraw');
$withdrawalWins = crm_sms_consent_payload('contact');
test_expect($withdrawalWins['sms'] === false, 'An explicit withdrawal must win over a stale checked value.');

$_SERVER['CONTENT_LENGTH'] = '1025';
test_expect(!crm_request_shape_is_bounded(1024, 10), 'Oversized request bodies must be rejected.');
unset($_SERVER['CONTENT_LENGTH']);
$_SERVER['HTTP_HOST'] = '3dpreciscan.com';
$_SERVER['HTTP_REFERER'] = 'https://3dpreciscan.com/en/contact-us?campaign=test';
test_expect(crm_form_return_path('/contactez-nous') === '/en/contact-us', 'Same-origin forms should receive errors on their actual path.');
$_SERVER['HTTP_REFERER'] = 'https://attacker.example/phish';
test_expect(crm_form_return_path('/contactez-nous') === '/contactez-nous', 'Cross-origin return paths must fail closed.');

putenv('CRM_TRUSTED_PROXY_IPS=10.0.0.2');
$_SERVER['REMOTE_ADDR'] = '10.0.0.2';
$_SERVER['HTTP_X_FORWARDED_FOR'] = '198.51.100.25, 10.0.0.2';
test_expect(crm_request_ip() === '198.51.100.25', 'Configured proxies should yield the original client IP.');
$_SERVER['REMOTE_ADDR'] = '198.51.100.40';
$_SERVER['HTTP_X_FORWARDED_FOR'] = '198.51.100.99';
test_expect(crm_request_ip() === '198.51.100.40', 'Forwarded headers from untrusted peers must be ignored.');
putenv('CRM_TRUSTED_PROXY_IPS');

test_expect(crm_form_security_configuration_errors(false) === array(), 'Local forms may run without Turnstile or Redis.');
test_expect(crm_turnstile_script_html() === '', 'Unconfigured local forms must not load the Turnstile script.');
test_expect(crm_turnstile_widget_html('contact') === '', 'Unconfigured local forms must not render a Turnstile field.');
$_POST = array();
test_expect(crm_turnstile_submission_is_valid('contact'), 'Turnstile must remain optional for unconfigured local development.');

putenv('CRM_FORM_PRODUCTION=false');
putenv('CRM_FORM_ENV=production');
test_expect(crm_form_is_production(), 'A production environment must not be downgraded by a stale false override.');
$productionErrors = crm_form_security_configuration_errors(false);
test_expect(in_array('TURNSTILE_SITE_KEY', $productionErrors, true), 'Production must require a Turnstile site key.');
test_expect(in_array('TURNSTILE_SECRET_KEY', $productionErrors, true), 'Production must require a Turnstile secret key.');
test_expect(in_array('CRM_RATE_LIMIT_REDIS_URL', $productionErrors, true), 'Production must require a Redis limiter URL.');
test_expect(in_array('phpredis extension', $productionErrors, true), 'Production must require the phpredis extension.');
test_expect(!crm_rate_limit_allow('production-no-local-fallback', 2, 60), 'Production must refuse the process-local file limiter.');

putenv('TURNSTILE_SITE_KEY=1x00000000000000000000AA');
putenv('TURNSTILE_SECRET_KEY=1x0000000000000000000000000000000AA');
putenv('TURNSTILE_EXPECTED_HOSTNAMES=forms.example.com');
putenv('CRM_RATE_LIMIT_REDIS_URL=rediss://limiter:secret@redis.example.com:6380/3');
test_expect(crm_form_security_configuration_errors(true) === array(), 'Complete production bot and limiter settings must validate.');
$redisConfiguration = crm_rate_limit_redis_configuration();
test_expect(is_array($redisConfiguration), 'A valid Redis TLS URL must be parsed.');
test_expect($redisConfiguration['scheme'] === 'rediss', 'The Redis TLS scheme must be retained.');
test_expect($redisConfiguration['database'] === 3, 'The Redis database number must be parsed.');

putenv('CRM_FORM_ENV=local');
$scriptHtml = crm_turnstile_script_html();
$widgetHtml = crm_turnstile_widget_html('contact');
test_expect(strpos($scriptHtml, 'https://challenges.cloudflare.com/turnstile/v0/api.js') !== false, 'Configured forms must load the official Turnstile script.');
test_expect(strpos($widgetHtml, 'data-sitekey="1x00000000000000000000AA"') !== false, 'The widget must render the configured public site key.');
test_expect(strpos($widgetHtml, 'data-response-field-name="cf-turnstile-response"') !== false, 'The widget must post the standard response field.');
test_expect(strpos($widgetHtml, '1x0000000000000000000000000000000AA') === false, 'The Turnstile secret must never be rendered.');

$_SERVER['REMOTE_ADDR'] = '198.51.100.77';
$_POST = array('cf-turnstile-response' => 'valid-test-token');
$turnstileRequest = null;
$turnstileTransport = function ($url, $fields) use (&$turnstileRequest) {
	$turnstileRequest = array('url' => $url, 'fields' => $fields);
	return array('success' => true, 'action' => 'contact', 'hostname' => 'forms.example.com', 'error-codes' => array());
};
test_expect(crm_turnstile_submission_is_valid('contact', $turnstileTransport), 'A valid Siteverify response must authorize submission.');
test_expect($turnstileRequest['url'] === 'https://challenges.cloudflare.com/turnstile/v0/siteverify', 'Verification must use Cloudflare Siteverify.');
test_expect($turnstileRequest['fields']['secret'] === '1x0000000000000000000000000000000AA', 'Siteverify must receive the server-side secret.');
test_expect($turnstileRequest['fields']['response'] === 'valid-test-token', 'Siteverify must receive the browser token.');
test_expect($turnstileRequest['fields']['remoteip'] === '198.51.100.77', 'Siteverify should receive the trustworthy request IP.');
test_expect(crm_valid_submission_id($turnstileRequest['fields']['idempotency_key']), 'Siteverify must receive a UUID idempotency key.');

$wrongActionTransport = function ($url, $fields) {
	return array('success' => true, 'action' => 'quote', 'hostname' => 'forms.example.com');
};
test_expect(!crm_turnstile_submission_is_valid('contact', $wrongActionTransport), 'A Siteverify action mismatch must fail closed.');
$_POST = array('cf-turnstile-response' => str_repeat('a', 2049));
test_expect(!crm_turnstile_submission_is_valid('contact', $turnstileTransport), 'Tokens above Cloudflare\'s 2048-character limit must be rejected locally.');

class TestRedisRateLimiter
{
	public $calls = array();
	public $count = 0;
	public function eval($script, $arguments, $keyCount)
	{
		$this->calls[] = array($script, $arguments, $keyCount);
		$this->count++;
		return $this->count;
	}
}

$fakeRedis = new TestRedisRateLimiter();
$_SERVER['REMOTE_ADDR'] = '192.0.2.44';
test_expect(crm_rate_limit_allow_redis('shared-test', 2, 60, $fakeRedis) === true, 'Redis must admit the first request.');
test_expect(crm_rate_limit_allow_redis('shared-test', 2, 60, $fakeRedis) === true, 'Redis must admit requests within the limit.');
test_expect(crm_rate_limit_allow_redis('shared-test', 2, 60, $fakeRedis) === false, 'Redis must reject requests above the limit.');
test_expect(strpos($fakeRedis->calls[0][0], "redis.call('INCR'") !== false, 'The distributed limiter must use one atomic Redis script.');
test_expect($fakeRedis->calls[0][2] === 1, 'The Redis script must operate on one hashed bucket key.');
test_expect(strpos($fakeRedis->calls[0][1][0], '192.0.2.44') === false, 'Redis keys must not expose raw client IPs.');

putenv('TURNSTILE_SITE_KEY');
putenv('TURNSTILE_SECRET_KEY');
putenv('TURNSTILE_EXPECTED_HOSTNAMES');
putenv('CRM_RATE_LIMIT_REDIS_URL');

$rateDirectory = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'crm-rate-test-' . bin2hex(random_bytes(6));
putenv('CRM_RATE_LIMIT_DIR=' . $rateDirectory);
$_SERVER['REMOTE_ADDR'] = '192.0.2.10';
test_expect(crm_rate_limit_allow('test', 2, 60), 'First request should pass the rate limit.');
test_expect(crm_rate_limit_allow('test', 2, 60), 'Second request should pass the rate limit.');
test_expect(!crm_rate_limit_allow('test', 2, 60), 'Third request should be rate limited.');
$_SERVER['REMOTE_ADDR'] = '192.0.2.11';
test_expect(crm_rate_limit_allow('test', 2, 60), 'Rate limiting must be isolated per IP.');

$rateFile = $rateDirectory . DIRECTORY_SEPARATOR . 'buckets.json';
if (is_file($rateFile)) unlink($rateFile);
if (is_dir($rateDirectory)) rmdir($rateDirectory);
putenv('CRM_RATE_LIMIT_DIR');

$handlerActions = array(
	'send-email.php' => 'contact',
	'send-email-services.php' => 'services',
	'send-email-soumission.php' => 'quote',
);
foreach ($handlerActions as $handlerName => $action) {
	$handlerSource = file_get_contents(dirname(__DIR__) . DIRECTORY_SEPARATOR . $handlerName);
	$configPosition = strpos($handlerSource, 'crm_require_form_security_configuration()');
	$verificationPosition = strpos($handlerSource, "crm_turnstile_submission_is_valid('" . $action . "')");
	$deliveryPositions = array_filter(array(
		strpos($handlerSource, 'crm_send_html_mail('),
		strpos($handlerSource, '$mailSent = mail('),
		strpos($handlerSource, 'crm_send_lead_webhook('),
	), function ($position) { return $position !== false; });
	$deliveryPosition = empty($deliveryPositions) ? false : min($deliveryPositions);
	test_expect($configPosition !== false, $handlerName . ' must validate production security configuration.');
	test_expect($verificationPosition !== false, $handlerName . ' must validate its Turnstile action.');
	test_expect($deliveryPosition !== false && $verificationPosition < $deliveryPosition, $handlerName . ' must verify Turnstile before delivery.');
}
$contactSource = file_get_contents(dirname(__DIR__) . '/contact.php');
$quoteSource = file_get_contents(dirname(__DIR__) . '/soumission.php');
test_expect(strpos($contactSource, "crm_turnstile_widget_html('contact')") !== false, 'The contact form must include its Turnstile widget.');
test_expect(strpos($quoteSource, "crm_turnstile_widget_html('quote')") !== false, 'The quote form must include its Turnstile widget.');

putenv('CRM_FORM_ENV');
putenv('CRM_FORM_PRODUCTION');

ini_set('error_log', $previousErrorLog === false ? '' : $previousErrorLog);
if ($testErrorLog !== false && is_file($testErrorLog)) unlink($testErrorLog);

if (!empty($failures)) {
	foreach ($failures as $failure) fwrite(STDERR, 'FAIL: ' . $failure . PHP_EOL);
	exit(1);
}

echo "PHP security helper tests passed.\n";
