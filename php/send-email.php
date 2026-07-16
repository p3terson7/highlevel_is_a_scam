<?php

error_reporting(E_ALL);
ini_set('display_errors', '0');
ini_set('log_errors', '1');

require_once __DIR__ . '/fonctions.php';

crm_require_post();

$lang = crm_post_enum('lang', array('fr', 'en'), 'fr');
if ($lang === false) $lang = 'fr';
$devis = array_key_exists('sEntreprise', $_POST);
$formPath = crm_form_return_path($devis ? ($lang === 'en' ? '/en/quote' : '/soumission') : ($lang === 'en' ? '/en/contact' : '/contactez-nous'));

if (!crm_request_shape_is_bounded(64 * 1024, 32)) {
	error_log('send-email.php: request rejected by size or field-count limit.');
	crm_redirect($formPath, 'validation');
}
crm_require_form_security_configuration();
if (!crm_rate_limit_allow('contact', 6, 600)) {
	error_log('send-email.php: request rejected by rate limit.');
	crm_redirect($formPath, 'rate');
}
if (crm_honeypot_triggered()) {
	error_log('send-email.php: honeypot submission rejected.');
	crm_redirect('/');
}
if (!crm_turnstile_submission_is_valid('contact')) {
	error_log('send-email.php: Turnstile submission rejected.');
	crm_redirect($formPath, 'verification');
}

$errors = array();
$submissionId = crm_submission_id_from_post();
$nom = crm_post_string('sNom', 160);
$entreprise = $devis ? crm_post_string('sEntreprise', 160) : '';
$email = crm_post_string('sCourriel', 254);
$tel = crm_post_string('sTel', 32);
$subjectInput = crm_post_string('subject', 160);
$msg = crm_post_string('sMessage', 5000);

if ($submissionId === false) $errors[] = 'submission_id';
if ($nom === false || $nom === '') $errors[] = 'name';
if ($entreprise === false) $errors[] = 'company';
if ($email === false || !crm_valid_email($email)) $errors[] = 'email';
if ($tel === false || !crm_valid_phone($tel)) $errors[] = 'phone';
if ($subjectInput === false || $subjectInput === '' || !crm_valid_single_line($subjectInput)) $errors[] = 'subject';
if ($msg === false) $errors[] = 'message';

if (!empty($errors)) {
	error_log('send-email.php: submission rejected by validation.');
	crm_redirect($formPath, 'validation', is_string($submissionId) ? $submissionId : '');
}

$smsConsent = crm_sms_consent_payload($devis ? 'quote_contact' : 'contact');
$sourcePageUrl = crm_source_page_url();
$referrerUrl = crm_referrer_url();
$mailSubject = '3DPreciscan - Courriel concernant: ' . $subjectInput;

$message = "<html><head><meta charset='UTF-8'></head>
<body style='background-color:#eaeaea; padding:10px;'>
<table width='100%' border='0' align='center' cellpadding='4' bgcolor='#FFFFFF' style='border:1px solid #d7d7d7; font-family:Arial, Helvetica, sans-serif; font-size:12px; color:#333;'>
<tr><td colspan='2'><strong>Courriel provenant du formulaire du site Web</strong></td></tr>
<tr><td width='30%'>Identifiant :</td><td>" . crm_html($submissionId) . "</td></tr>
<tr><td>Nom :</td><td>" . crm_html($nom) . "</td></tr>
<tr><td>Entreprise :</td><td>" . crm_html($entreprise) . "</td></tr>
<tr><td>Courriel :</td><td>" . crm_html($email) . "</td></tr>
<tr><td>Téléphone :</td><td>" . crm_html($tel) . "</td></tr>
<tr><td>Sujet :</td><td>" . crm_html($subjectInput) . "</td></tr>
<tr><td>Message :</td><td>" . nl2br(crm_html($msg), false) . "</td></tr>
<tr><td>Langue :</td><td>" . crm_html($lang) . "</td></tr>
</table></body></html>";

$mailSent = false;
$crmSent = false;
try {
	$mailSent = crm_send_html_mail(
		'fabien.lagier@3dpreciscan.com, dacampos@publissoft.ca',
		$mailSubject,
		$message,
		$email
	);
	$crmSent = crm_send_lead_webhook(array(
		'submission_id' => $submissionId,
		'external_lead_id' => $submissionId,
		'source_page_url' => $sourcePageUrl,
		'referrer' => $referrerUrl,
		'consent' => $smsConsent,
		'lead' => array(
			'id' => $submissionId,
			'full_name' => $nom,
			'phone' => $tel,
			'email' => $email,
		),
		'form_answers' => array(
			'form_type' => $devis ? 'quote_contact' : 'contact',
			'full_name' => $nom,
			'company' => $entreprise,
			'email' => $email,
			'phone' => $tel,
			'subject' => $subjectInput,
			'message' => $msg,
			'lang' => $lang,
			'source_page_url' => $sourcePageUrl,
			'referrer_url' => $referrerUrl,
		),
		'tracking' => crm_tracking_payload(),
	));
} catch (Throwable $exception) {
	error_log('send-email.php: delivery raised an exception for submission=' . $submissionId . '.');
}

crm_log_delivery('send-email.php', $submissionId, $mailSent, $crmSent);
if (!$mailSent && !$crmSent) crm_redirect($formPath, 'delivery', $submissionId);

if ($devis) {
	crm_redirect($lang === 'en' ? '/en/thanks-quote' : '/merci-soumission');
}
crm_redirect($lang === 'en' ? '/en/thanks' : '/merci-contact');
