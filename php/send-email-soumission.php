<?php

error_reporting(E_ALL);
ini_set('display_errors', '0');
ini_set('log_errors', '1');

require_once __DIR__ . '/fonctions.php';

crm_require_post();

$lang = crm_post_enum('lang', array('fr', 'en'), 'fr');
if ($lang === false) $lang = 'fr';
$formPath = crm_form_return_path($lang === 'en' ? '/en/quote' : '/soumission');

if (!crm_request_shape_is_bounded(18 * 1024 * 1024, 64)) {
	error_log('send-email-soumission.php: request rejected by size or field-count limit.');
	crm_redirect($formPath, 'files');
}
crm_require_form_security_configuration();
if (!crm_rate_limit_allow('quote', 4, 900)) {
	error_log('send-email-soumission.php: request rejected by rate limit.');
	crm_redirect($formPath, 'rate');
}
if (crm_honeypot_triggered()) {
	error_log('send-email-soumission.php: honeypot submission rejected.');
	crm_redirect('/');
}
if (!crm_turnstile_submission_is_valid('quote')) {
	error_log('send-email-soumission.php: Turnstile submission rejected.');
	crm_redirect($formPath, 'verification');
}
$errors = array();
$submissionId = crm_submission_id_from_post();
$typeClient = crm_post_enum('type_client', array('Company', 'Individual'));
$name = crm_post_string('sNom', 160);
$phone = crm_post_string('sTel', 32);
$email = crm_post_string('sCourriel', 254);
$height = crm_post_string('hauteur', 100);
$width = crm_post_string('largeur', 100);
$length = crm_post_string('longueur', 100);
$otherDimensions = crm_post_string('autres', 160, '');
$deadline = crm_post_string('sDelai', 160, '');
$urgent = crm_post_enum('urgent', array('yes', 'no'));
$information = crm_post_string('info', 5000, '');

if ($submissionId === false) $errors[] = 'submission_id';
if ($typeClient === false) $errors[] = 'type_client';
if ($name === false || crm_string_length($name) < 3) $errors[] = 'name';
if ($phone === false || !crm_valid_phone($phone)) $errors[] = 'phone';
if ($email === false || !crm_valid_email($email)) $errors[] = 'email';
if ($height === false || $height === '') $errors[] = 'height';
if ($width === false || $width === '') $errors[] = 'width';
if ($length === false || $length === '') $errors[] = 'length';
if ($otherDimensions === false) $errors[] = 'other_dimensions';
if ($deadline === false) $errors[] = 'deadline';
if ($urgent === false) $errors[] = 'urgent';
if ($information === false) $errors[] = 'information';

$serviceFields = array(
	'Scan3d' => 'Scan 3D',
	'Rec3d' => 'Reconstruction 3D',
	'R_ing3d2d' => 'Rétro-ingénierie (3D & 2D)',
	'Inspection' => 'Inspection 3D',
	'Metrologie' => 'Métrologie industrielle',
	'Concep2d3d' => 'Design (3D & 2D)',
	'Modelisation3D' => 'Modélisation',
	'Ingenerie' => 'Ingénierie',
	'Imp3d' => 'Impression 3D',
	'Simul3D' => 'Simulation 3D',
	'Aucun' => 'Je ne sais pas',
);
$services = array();
foreach ($serviceFields as $field => $label) {
	if (!isset($_POST[$field])) continue;
	if (!is_string($_POST[$field]) || !in_array($_POST[$field], array('yesclbase', 'yes', 'on', '1'), true)) {
		$errors[] = 'services';
		continue;
	}
	$services[] = $label;
}
if (empty($services)) $errors[] = 'services';

if (!empty($errors)) {
	error_log('send-email-soumission.php: submission rejected by validation.');
	crm_redirect($formPath, 'validation', is_string($submissionId) ? $submissionId : '');
}

$storedFiles = array();
$uploadsCleaned = false;
register_shutdown_function(function () use (&$storedFiles, &$uploadsCleaned) {
	if (!$uploadsCleaned && !empty($storedFiles)) crm_cleanup_uploads($storedFiles);
});

$uploadResult = crm_store_quote_uploads(isset($_FILES['images']) ? $_FILES['images'] : array(), $storedFiles);
$storedFiles = $uploadResult['files'];
if (!empty($uploadResult['errors'])) {
	crm_cleanup_uploads($storedFiles);
	$storedFiles = array();
	$uploadsCleaned = true;
	error_log('send-email-soumission.php: attachment validation failed.');
	crm_redirect($formPath, 'files', $submissionId);
}

$fileNames = array();
foreach ($storedFiles as $storedFile) $fileNames[] = $storedFile['name'];
$servicesSummary = implode(', ', $services);
$displayOtherDimensions = $otherDimensions !== '' ? $otherDimensions : 'Aucune';
$displayDeadline = $deadline !== '' ? $deadline : 'Non spécifié';
$typeClientLabel = $typeClient === 'Company' ? 'Entreprise' : 'Particulier';
$smsConsent = crm_sms_consent_payload('quote_request');
$sourcePageUrl = crm_source_page_url();
$referrerUrl = crm_referrer_url();

$message = "<html><head><meta charset='UTF-8'></head>
<body style='background-color:#f4f6f9; padding:20px; font-family:Arial,sans-serif;'>
<div style='max-width:650px; margin:0 auto; background:#fff; border:1px solid #e2e8f0; border-radius:8px; overflow:hidden;'>
<div style='background:#0f172a; padding:24px; text-align:center; border-bottom:4px solid #3b82f6;'>
<h2 style='color:#fff; margin:0;'>3D PreciScan</h2><p style='color:#94a3b8;'>Nouvelle demande de soumission</p></div>
<div style='padding:28px;'>"
	. ($urgent === 'yes' ? "<p style='background:#fef2f2; border-left:4px solid #ef4444; color:#991b1b; padding:12px;'><strong>Demande urgente</strong></p>" : '') .
"<table width='100%' cellpadding='7' style='font-size:14px; color:#334155;'>
<tr><td width='35%'><strong>Identifiant</strong></td><td>" . crm_html($submissionId) . "</td></tr>
<tr><td><strong>Secteur</strong></td><td>" . crm_html($typeClientLabel) . "</td></tr>
<tr><td><strong>Nom / entreprise</strong></td><td>" . crm_html($name) . "</td></tr>
<tr><td><strong>Téléphone</strong></td><td>" . crm_html($phone) . "</td></tr>
<tr><td><strong>Courriel</strong></td><td>" . crm_html($email) . "</td></tr>
<tr><td><strong>Services</strong></td><td>" . crm_html($servicesSummary) . "</td></tr>
<tr><td><strong>Délai</strong></td><td>" . crm_html($displayDeadline) . "</td></tr>
<tr><td><strong>Dimensions</strong></td><td>H " . crm_html($height) . " / L " . crm_html($width) . " / Long. " . crm_html($length) . " / Autres " . crm_html($displayOtherDimensions) . "</td></tr>
<tr><td><strong>Informations</strong></td><td>" . nl2br(crm_html($information), false) . "</td></tr>
</table></div></div></body></html>";

$boundary = '=_3dpreciscan_' . bin2hex(random_bytes(18));
$mailBody = '--' . $boundary . "\r\n";
$mailBody .= "Content-Type: text/html; charset=UTF-8\r\n";
$mailBody .= "Content-Transfer-Encoding: 8bit\r\n\r\n";
$mailBody .= $message . "\r\n";

$attachmentError = false;
foreach ($storedFiles as $storedFile) {
	$content = @file_get_contents($storedFile['path']);
	if ($content === false || strlen($content) !== (int)$storedFile['size']) {
		$attachmentError = true;
		break;
	}
	$mailBody .= '--' . $boundary . "\r\n";
	$mailBody .= 'Content-Type: ' . $storedFile['mime'] . '; name="' . $storedFile['name'] . "\"\r\n";
	$mailBody .= "Content-Transfer-Encoding: base64\r\n";
	$mailBody .= 'Content-Disposition: attachment; filename="' . $storedFile['name'] . "\"\r\n\r\n";
	$mailBody .= chunk_split(base64_encode($content)) . "\r\n";
	unset($content);
}
$mailBody .= '--' . $boundary . "--\r\n";

if ($attachmentError) {
	error_log('send-email-soumission.php: a stored attachment became unreadable.');
	crm_cleanup_uploads($storedFiles);
	$storedFiles = array();
	$uploadsCleaned = true;
	crm_redirect($formPath, 'files', $submissionId);
}

$mailSent = false;
$crmSent = false;
try {
	$from = crm_mail_from_address();
	if ($from !== false) {
		$headers = 'From: 3D PreciScan <' . $from . ">\r\n";
		$headers .= 'Reply-To: ' . $email . "\r\n";
		$headers .= "MIME-Version: 1.0\r\n";
		$headers .= 'Content-Type: multipart/mixed; boundary="' . $boundary . '"';
		$mailSent = mail(
			'fabien.lagier@3dpreciscan.com, dacampos@publissoft.ca',
			'3DPreciscan - Formulaire soumission',
			$mailBody,
			$headers,
			'-f' . $from
		);
	}

	$crmSent = crm_send_lead_webhook(array(
		'submission_id' => $submissionId,
		'external_lead_id' => $submissionId,
		'source_page_url' => $sourcePageUrl,
		'referrer' => $referrerUrl,
		'consent' => $smsConsent,
		'lead' => array(
			'id' => $submissionId,
			'full_name' => $name,
			'phone' => $phone,
			'email' => $email,
		),
		'form_answers' => array(
			'form_type' => 'quote_request',
			'type_client' => $typeClient,
			'full_name' => $name,
			'phone' => $phone,
			'email' => $email,
			'hauteur' => $height,
			'largeur' => $width,
			'longueur' => $length,
			'autres_dimensions' => $otherDimensions,
			'delai_souhaite' => $deadline,
			'urgent' => $urgent,
			'services' => $services,
			'services_summary' => $servicesSummary,
			'informations_additionnelles' => $information,
			'fichiers_joints' => $fileNames,
			'lang' => $lang,
			'source_page_url' => $sourcePageUrl,
			'referrer_url' => $referrerUrl,
		),
		'tracking' => crm_tracking_payload(),
	));
} catch (Throwable $exception) {
	error_log('send-email-soumission.php: delivery raised an exception for submission=' . $submissionId . '.');
}

crm_cleanup_uploads($storedFiles);
$storedFiles = array();
$uploadsCleaned = true;
crm_log_delivery('send-email-soumission.php', $submissionId, $mailSent, $crmSent);

if (!$mailSent && !$crmSent) crm_redirect($formPath, 'delivery', $submissionId);
crm_redirect($lang === 'en' ? '/en/thanks' : '/merci');
