<?php

$publicRoot = dirname(__DIR__) . '/public_html';
require_once $publicRoot . '/php/fonctions.php';

$failures = array();

function test_expect($condition, $message)
{
	global $failures;
	if (!$condition) $failures[] = $message;
}

function test_header_value($headers, $name)
{
	$prefix = strtolower((string)$name) . ':';
	foreach ((array)$headers as $header) {
		if (!is_string($header) || strpos(strtolower($header), $prefix) !== 0) continue;
		return trim(substr($header, strlen($prefix)));
	}
	return '';
}

$pageHelperPath = realpath($publicRoot . '/pages/../php/fonctions.php');
test_expect($pageHelperPath === realpath($publicRoot . '/php/fonctions.php'), 'The page-level helper path must resolve to public_html/php/fonctions.php.');

foreach (array('soumission.php', 'contact.php') as $pageName) {
	$pagePath = $publicRoot . '/pages/' . $pageName;
	$pageSource = is_file($pagePath) ? file_get_contents($pagePath) : '';
	test_expect($pageSource !== '', $pageName . ' must exist below public_html/pages.');
	$bootstrapPosition = strpos($pageSource, 'includes/tophead.php');
	$helperPosition = strpos($pageSource, "require_once dirname(__DIR__) . '/php/fonctions.php';");
	test_expect(
		$helperPosition !== false,
		$pageName . ' must load the form helper from the sibling public_html/php directory.'
	);
	test_expect(
		$bootstrapPosition !== false && $helperPosition !== false && $bootstrapPosition < $helperPosition,
		$pageName . ' must load the legacy site bootstrap before the guarded form helper.'
	);
}

$helperSource = file_get_contents($publicRoot . '/php/fonctions.php');
foreach (array('nettoyage', 'truncate_text', 'remove_curly_quotes') as $legacyFunction) {
	test_expect(
		strpos($helperSource, "if (!function_exists('" . $legacyFunction . "'))") !== false,
		$legacyFunction . ' must be guarded against the existing cPanel bootstrap definition.'
	);
}

$quoteSource = file_get_contents($publicRoot . '/pages/soumission.php');
test_expect(strpos($quoteSource, 'action="/php/send-email-soumission.php"') !== false, 'The quote form action must target the public_html/php handler.');
test_expect(strpos($quoteSource, '<button type="submit" class="btn btn--primary form-submit-btn"') !== false, 'The quote form must retain the themed submit button.');
test_expect(strpos($quoteSource, "input.checkValidity()") !== false, 'The quote form must retain native field validation.');

$handlers = array(
	'send-email.php' => 'crm_send_html_mail(',
	'send-email-services.php' => 'crm_send_html_mail(',
	'send-email-soumission.php' => '$mailSent = mail(',
);
foreach ($handlers as $handlerName => $mailCall) {
	$handlerPath = $publicRoot . '/php/' . $handlerName;
	$handlerSource = is_file($handlerPath) ? file_get_contents($handlerPath) : '';
	test_expect($handlerSource !== '', $handlerName . ' must exist below public_html/php.');
	test_expect(strpos($handlerSource, "require_once __DIR__ . '/fonctions.php';") !== false, $handlerName . ' must load the adjacent form helper.');
	$mailPosition = strpos($handlerSource, $mailCall);
	$crmPosition = strpos($handlerSource, 'crm_send_lead_webhook(');
	test_expect(
		$mailPosition !== false && $crmPosition !== false && $mailPosition < $crmPosition,
		$handlerName . ' must complete the established email attempt before starting the additive CRM request.'
	);
	test_expect(
		substr_count($handlerSource, $mailCall) === 1,
		$handlerName . ' must attempt its established email delivery exactly once.'
	);
	test_expect(
		substr_count($handlerSource, 'crm_send_lead_webhook(') === 1,
		$handlerName . ' must attempt the additive CRM delivery exactly once.'
	);
	test_expect(
		strpos($handlerSource, 'crm_delivery_is_complete($crmSent, $mailSent,') !== false,
		$handlerName . ' must preserve the independent email fallback when CRM delivery is unavailable.'
	);
	test_expect(
		strpos($handlerSource, "'form_type'") === false,
		$handlerName . ' must not expose internal form routing as a customer form answer.'
	);
}

$emailContracts = array(
	'send-email.php' => array(
		"\$mailSubject = '3DPreciscan - Courriel concernant: ' . \$subjectInput;",
		"'fabien.lagier@3dpreciscan.com, dacampos@publissoft.ca'",
	),
	'send-email-services.php' => array(
		"\$mailSubject = '3DPreciscan - Courriel concernant: ' . \$subjectInput;",
		"crm_send_html_mail('fabien.lagier@3dpreciscan.com'",
	),
	'send-email-soumission.php' => array(
		"'3DPreciscan - Formulaire soumission'",
		"'fabien.lagier@3dpreciscan.com, dacampos@publissoft.ca'",
	),
);
foreach ($emailContracts as $handlerName => $expectedFragments) {
	$handlerSource = file_get_contents($publicRoot . '/php/' . $handlerName);
	foreach ($expectedFragments as $expectedFragment) {
		test_expect(
			strpos($handlerSource, $expectedFragment) !== false,
			$handlerName . ' must preserve its established recipient and subject contract.'
		);
	}
}

$thankPages = array('merci.php', 'merci-contact.php', 'merci-soumission.php', 'merci-services.php');
foreach ($thankPages as $thankPage) {
	$thankPagePath = $publicRoot . '/pages/' . $thankPage;
	$thankPageSource = is_file($thankPagePath) ? file_get_contents($thankPagePath) : '';
	test_expect($thankPageSource !== '', $thankPage . ' must exist below public_html/pages.');
	test_expect(strpos($thankPageSource, '<article class="form-card confirmation-card">') !== false, $thankPage . ' must contain its complete confirmation layout.');
	test_expect(strpos($thankPageSource, 'merci-content.php') === false, $thankPage . ' must not depend on the removed helper partial.');
	test_expect(strpos($thankPageSource, 'landing-page-fixes.css') === false, $thankPage . ' must not depend on the removed stylesheet.');
	test_expect(stripos($thankPageSource, 'avoir contacter') === false, $thankPage . ' must not contain the old infinitive error.');
	test_expect(stripos($thankPageSource, 'appeler nous') === false, $thankPage . ' must not contain the old imperative error.');
	test_expect(stripos($thankPageSource, 'confimation') === false, $thankPage . ' must not contain the old spelling error.');
}

$serviceHandler = file_get_contents($publicRoot . '/php/send-email-services.php');
test_expect(strpos($serviceHandler, "'/merci-services'") !== false, 'French service submissions must redirect to the existing service confirmation page.');

putenv('CRM_WEBHOOK_URL=https://leadops-console.onrender.com/webhooks/form/3d-preciscan');
putenv('CRM_WEBHOOK_SECRET=layout-test-secret');
test_expect(crm_webhook_url() === 'https://leadops-console.onrender.com/webhooks/form/3d-preciscan', 'The PHP relay must accept only the configured 3D PreciScan endpoint.');
test_expect(crm_webhook_client_key_from_url(crm_webhook_url()) === '3d-preciscan', 'The CRM endpoint must resolve the correct client key.');
test_expect(crm_webhook_response_is_accepted(202, '{"status":"accepted","client_key":"3d-preciscan"}', '3d-preciscan'), 'The expected Render acceptance response must pass.');
test_expect(!crm_webhook_response_is_accepted(200, '<html>OK</html>', '3d-preciscan'), 'Arbitrary HTML responses must not count as CRM delivery.');
test_expect(crm_delivery_is_complete(true, false, false), 'CRM acceptance must complete a submission without attachments.');
test_expect(crm_delivery_is_complete(false, true, false), 'Email acceptance must remain a working fallback when CRM delivery is unavailable.');
test_expect(!crm_delivery_is_complete(false, false, false), 'A submission must fail when neither CRM nor email accepts it.');
test_expect(!crm_delivery_is_complete(true, false, true), 'A quote with attachments must require attachment email delivery.');
test_expect(crm_delivery_is_complete(false, true, true), 'A quote with attachments must complete when its attachment email is accepted.');

$metaTracking = array(
	'utm_source' => 'meta',
	'utm_medium' => 'paid_social',
	'utm_campaign' => 'QC | Scan 3D | July 2026',
	'utm_content' => 'Prospecting | Engineers',
	'utm_term' => 'Video | Replacement part',
	'utm_id' => '120999000111222',
	'ad_id' => '120999000333444',
);
$metaLandingUrl = 'https://3dpreciscan.com/soumission?'
	. http_build_query($metaTracking, '', '&', PHP_QUERY_RFC3986);
$parsedMetaTracking = array();
parse_str((string)parse_url($metaLandingUrl, PHP_URL_QUERY), $parsedMetaTracking);
test_expect($parsedMetaTracking === $metaTracking, 'The representative Meta campaign URL must preserve every expanded UTM and ad identifier.');

$originalPost = $_POST;
$originalReferer = isset($_SERVER['HTTP_REFERER']) ? $_SERVER['HTTP_REFERER'] : null;
$_POST = array_merge(
	$parsedMetaTracking,
	array(
		'source_page_url' => $metaLandingUrl,
		'referrer_url' => 'https://www.facebook.com/',
	)
);
$_SERVER['HTTP_REFERER'] = 'https://www.facebook.com/';

$postedTracking = crm_tracking_payload();
test_expect($postedTracking === $metaTracking, 'The PHP form handler must preserve Meta tracking fields posted by the landing page.');
test_expect(crm_source_page_url() === $metaLandingUrl, 'The PHP form handler must preserve the complete Meta landing URL.');
test_expect(crm_referrer_url() === 'https://www.facebook.com/', 'The PHP form handler must preserve the Meta referrer.');

$metaRelayPayload = array(
	'submission_id' => 'c0a8012e-1234-4abc-8def-0123456789ab',
	'external_lead_id' => 'c0a8012e-1234-4abc-8def-0123456789ab',
	'source_page_url' => crm_source_page_url(),
	'referrer' => crm_referrer_url(),
	'lead' => array(
		'full_name' => 'Meta Landing Test',
		'phone' => '+1 555 555 0100',
		'email' => 'meta-landing@example.invalid',
	),
	'form_answers' => array(
		'type_client' => 'Individual',
	),
	'tracking' => $postedTracking,
);
$capturedMetaRequests = array();
$metaTransport = function ($url, $headers, $json, $options, $attempt) use (&$capturedMetaRequests) {
	$capturedMetaRequests[] = array(
		'url' => $url,
		'headers' => $headers,
		'json' => $json,
		'options' => $options,
		'attempt' => $attempt,
	);
	return array(
		'status' => 202,
		'body' => '{"status":"accepted","client_key":"3d-preciscan"}',
		'transport_error' => false,
	);
};

$metaRelayAccepted = crm_send_lead_webhook($metaRelayPayload, $metaTransport);
test_expect($metaRelayAccepted, 'The signed Meta landing-page payload must accept the expected CRM 202 response.');
test_expect(count($capturedMetaRequests) === 1, 'An accepted Meta landing-page payload must be sent exactly once.');
if (count($capturedMetaRequests) === 1) {
	$capturedMetaRequest = $capturedMetaRequests[0];
	$capturedMetaBody = json_decode($capturedMetaRequest['json'], true);
	test_expect(
		is_array($capturedMetaBody) && isset($capturedMetaBody['tracking']) && $capturedMetaBody['tracking'] === $metaTracking,
		'The CRM JSON body must contain the complete Meta tracking map.'
	);
	test_expect(
		is_array($capturedMetaBody) && isset($capturedMetaBody['source_page_url']) && $capturedMetaBody['source_page_url'] === $metaLandingUrl,
		'The CRM JSON body must contain the complete Meta landing URL.'
	);
	test_expect(
		$capturedMetaRequest['url'] === 'https://leadops-console.onrender.com/webhooks/form/3d-preciscan',
		'The Meta landing-page payload must target the configured 3D PreciScan form endpoint.'
	);
	test_expect($capturedMetaRequest['attempt'] === 1, 'The accepted Meta landing-page payload must not be retried.');

	$timestamp = test_header_value($capturedMetaRequest['headers'], 'X-CRM-Webhook-Timestamp');
	$signature = test_header_value($capturedMetaRequest['headers'], 'X-CRM-Webhook-Signature');
	$expectedSignature = 'sha256=' . hash_hmac(
		'sha256',
		$timestamp . '.' . $capturedMetaRequest['json'],
		'layout-test-secret'
	);
	test_expect(ctype_digit($timestamp), 'The Meta relay must include a numeric CRM webhook timestamp.');
	test_expect(
		$signature !== '' && hash_equals($expectedSignature, $signature),
		'The Meta relay signature must cover the timestamp and exact JSON body.'
	);
}

$_POST = $originalPost;
if ($originalReferer === null) {
	unset($_SERVER['HTTP_REFERER']);
} else {
	$_SERVER['HTTP_REFERER'] = $originalReferer;
}

putenv('CRM_FORM_ENV=local');
putenv('CRM_WEBHOOK_SECRET');
$partialCrmConfigurationErrors = crm_form_security_configuration_errors(false);
test_expect(
	!in_array('CRM_WEBHOOK_SECRET', $partialCrmConfigurationErrors, true),
	'A missing optional CRM credential must not prevent the established email form from running.'
);
putenv('CRM_FORM_ENV');
putenv('CRM_WEBHOOK_URL');
putenv('CRM_WEBHOOK_SECRET');

if (!empty($failures)) {
	foreach ($failures as $failure) fwrite(STDERR, 'FAIL: ' . $failure . PHP_EOL);
	exit(1);
}

echo "PHP cPanel layout and form security tests passed.\n";
