<?php

error_reporting(E_ALL);
ini_set('display_errors', 0);
ini_set('log_errors', 1);

include ('fonctions.php');

// ── Honeypot ──

if (isset($_POST['phone']) && $_POST['phone'] !== '') {
	error_log('send-email.php: honeypot "phone" con valor "' . $_POST['phone'] . '" — posible bot o autofill del navegador.');
	header('Location: /'); 
	exit;
}

$devis = false;
$lang     = isset($_POST['lang'])       ? nettoyage($_POST['lang'])    : 'fr';
$subject  = isset($_POST['subject'])    ? nettoyage($_POST['subject']) : '';
$nom      = isset($_POST['sNom'])       ? nettoyage($_POST['sNom'])    : '';

if (isset($_POST['sEntreprise'])) {
	$entreprise = nettoyage($_POST['sEntreprise']);
	$devis = true;
} else {
	$entreprise = '';
}

$email = isset($_POST['sCourriel']) ? nettoyage($_POST['sCourriel']) : '';
$tel   = isset($_POST['sTel'])      ? nettoyage($_POST['sTel'])      : '';
$msg   = isset($_POST['sMessage'])  ? nettoyage($_POST['sMessage'])  : '';

$to = 'fabien.lagier@3dpreciscan.com, dacampos@publissoft.ca';
//$to = 'info@concepsim.com';

$subject = '3DPreciscan - Courriel concernant: ' . $subject;

$message = "<html><head></head>
<body style='background-color:#eaeaea; padding:10px;'>
<table width='100%' border='0' align='center' cellpadding='4' bgcolor='#FFFFFF' style='border:1px solid #d7d7d7; font-family:Arial, Helvetica, sans-serif; font-size:12px; color:#333;'>
<tr>
 <td colspan='2'>
	<table width='100%' border='0' cellspacing='0' cellpadding='0'>
		<tr><td height='25' align='left' valign='middle' style='color:#747474; border:1px solid #c1c1c1; padding-left:8px; background-color:#e0dfdf;'><strong>Courriel provenant du formulaire du site Web</strong></td></tr>
	</table>
 </td>
</tr>
	<tr><td width='30%'>Nom:-</td> <td width='65%'>$nom </td> </tr>
	<tr><td width='30%'>Entreprise:-</td> <td width='65%'>$entreprise </td> </tr>
	<tr><td>Courriel :-</td><td>$email</td></tr>
	<tr><td>Telephone:-</td><td>$tel</td></tr>
	<tr><td>Message :-</td><td>$msg</td></tr>
	<tr><td>Langue :-</td><td>$lang</td></tr>
</table></body></html>";

$headers  = 'MIME-Version: 1.0' . "\r\n";
$headers .= 'Content-type: text/html; charset=utf-8' . "\r\n";
$headers .= 'From: 3D PreciScan <' . $to . '>' . "\r\n";
$headers .= 'Reply-To: <' . $email . '>' . "\r\n";

$result = mail($to, $subject, $message, $headers, '-f ' . $to);

if ($result) {
	error_log('send-email.php: mail() OK -> ' . $to . ' | sujet: ' . $subject);
} else {
	error_log('send-email.php: mail() FALLÓ -> ' . $to . ' | sujet: ' . $subject);
}

crm_send_lead_webhook(array(
	'source_page_url' => crm_source_page_url(),
	'referrer' => crm_referrer_url(),
	'lead' => array(
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
		'subject' => $subject,
		'message' => $msg,
		'lang' => $lang,
		'source_page_url' => crm_source_page_url(),
		'referrer_url' => crm_referrer_url(),
	),
	'tracking' => crm_tracking_payload(),
));

if (!$devis) {
	if ($lang == 'fr')      header('Location: /merci-contact');
	elseif ($lang == 'en')  header('Location: /en/thanks');
	else                    header('Location: /merci-contact');
} else {
	if ($lang == 'fr')      header('Location: /merci-soumission');
	elseif ($lang == 'en')  header('Location: /en/thanks-quote');
	else                    header('Location: /merci-soumission');
}
exit;
