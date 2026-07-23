<?php

function nettoyage($var)
{
	return strip_tags($var);
}

function truncate_text($string, $limit, $break=' ', $pad='') 
{
	//Fonction pour afficher que les X premier caractère d'une string mais en ne coupant pas les mots
	// return with no change if string is shorter than $limit + $pad length
	if (mb_strlen($string) <= $limit + mb_strlen($pad))
		return $string;
	$string = mb_substr($string, 0, $limit);
	if (false !== ($breakpoint = mb_strrpos($string, $break)))
		$string = mb_substr($string, 0, $breakpoint);
	return trim($string).$pad;

}

function remove_curly_quotes($text) {
  // First, replace UTF-8 characters.
  $text = str_replace(array("\xe2\x80\x98", "\xe2\x80\x99", "\xe2\x80\x9c", "\xe2\x80\x9d", "\xe2\x80\x93", "\xe2\x80\x94", "\xe2\x80\xa6", "’", "’", "œ"), array("'", "'", '"', '"', '-', '--',
   '...', "'", "'", "oe"), $text);
  // Next, replace their Windows-1252 equivalents.
  $text = str_replace(array(chr(145), chr(146), chr(147), chr(148), chr(150), chr(151), chr(133)), array("'", "'", '"', '"', '-', '--', '...'), $text);

  
  return $text;
}

function crm_post_value($key, $default = '')
{
	return isset($_POST[$key]) ? trim(nettoyage($_POST[$key])) : $default;
}

function crm_post_raw_value($key, $default = '')
{
	return isset($_POST[$key]) ? trim($_POST[$key]) : $default;
}

function crm_tracking_payload()
{
	$keys = array('utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'utm_id', 'ad_id');
	$tracking = array();
	foreach ($keys as $key) {
		$tracking[$key] = crm_post_value($key);
	}
	return $tracking;
}

function crm_source_page_url()
{
	$posted = crm_post_raw_value('source_page_url');
	if ($posted !== '') return $posted;
	$referer = isset($_SERVER['HTTP_REFERER']) ? $_SERVER['HTTP_REFERER'] : '';
	return $referer;
}

function crm_referrer_url()
{
	$posted = crm_post_raw_value('referrer_url');
	if ($posted !== '') return $posted;
	return isset($_SERVER['HTTP_REFERER']) ? $_SERVER['HTTP_REFERER'] : '';
}

function crm_send_lead_webhook($payload)
{
	$crmWebhookUrl = 'https://leadops-console.onrender.com/webhooks/form/3d-preciscan';
	$crmWebhookSecret = ''; // Optional: set the same value in the CRM client webhook secret setting and here.

	$json = json_encode($payload, JSON_UNESCAPED_UNICODE);
	if ($json === false) {
		error_log('crm_send_lead_webhook: JSON encoding failed.');
		return false;
	}

	$headers = array('Content-Type: application/json');
	if ($crmWebhookSecret !== '') {
		$headers[] = 'X-CRM-Webhook-Secret: ' . $crmWebhookSecret;
	}

	if (function_exists('curl_init')) {
		$ch = curl_init($crmWebhookUrl);
		curl_setopt($ch, CURLOPT_POST, true);
		curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
		curl_setopt($ch, CURLOPT_POSTFIELDS, $json);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 3);
		curl_setopt($ch, CURLOPT_TIMEOUT, 5);
		$response = curl_exec($ch);
		$error = curl_error($ch);
		$status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
		curl_close($ch);

		if ($error || $status >= 400) {
			error_log('crm_send_lead_webhook: HTTP ' . $status . ' error=' . $error . ' response=' . $response);
			return false;
		}
		error_log('crm_send_lead_webhook: sent OK HTTP ' . $status);
		return true;
	}

	$context = stream_context_create(array(
		'http' => array(
				'method' => 'POST',
				'header' => implode("\r\n", $headers),
				'content' => $json,
				'timeout' => 5,
				'ignore_errors' => true,
			),
	));
	$response = @file_get_contents($crmWebhookUrl, false, $context);
	if ($response === false) {
		error_log('crm_send_lead_webhook: file_get_contents failed.');
		return false;
	}
	error_log('crm_send_lead_webhook: sent OK via stream.');
	return true;
}