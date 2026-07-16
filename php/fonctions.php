<?php

function nettoyage($var)
{
	if (!is_string($var)) return '';
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

function crm_string_length($value)
{
	return function_exists('mb_strlen') ? mb_strlen($value, 'UTF-8') : strlen($value);
}

function crm_html($value)
{
	return htmlspecialchars((string)$value, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
}

function crm_post_string($key, $maxLength, $default = '')
{
	if (!isset($_POST[$key])) return $default;
	if (!is_string($_POST[$key])) return false;
	$value = trim(strip_tags($_POST[$key]));
	if (crm_string_length($value) > (int)$maxLength) return false;
	return $value;
}

function crm_post_enum($key, $allowed, $default = false)
{
	$value = crm_post_string($key, 64, $default);
	if ($value === false || !in_array($value, $allowed, true)) return false;
	return $value;
}

function crm_valid_email($value)
{
	return is_string($value)
		&& strlen($value) <= 254
		&& strpos($value, "\r") === false
		&& strpos($value, "\n") === false
		&& filter_var($value, FILTER_VALIDATE_EMAIL) !== false;
}

function crm_valid_single_line($value)
{
	return is_string($value) && preg_match('/[\x00-\x1F\x7F]/', $value) !== 1;
}

function crm_valid_phone($value)
{
	if (!is_string($value) || crm_string_length($value) > 32) return false;
	if (!preg_match('/^[0-9+().\s-]+$/u', $value)) return false;
	$digits = preg_replace('/\D+/', '', $value);
	if (strlen($digits) < 7 || strlen($digits) > 20) return false;
	if (substr_count($value, '+') > 1) return false;
	if (strpos($value, '+') !== false && strpos(ltrim($value), '+') !== 0) return false;
	return true;
}

function crm_safe_web_url($value)
{
	if (!is_string($value) || $value === '' || strlen($value) > 2048) return '';
	if (filter_var($value, FILTER_VALIDATE_URL) === false) return '';
	$parts = parse_url($value);
	if (!is_array($parts) || !isset($parts['scheme'], $parts['host'])) return '';
	if (!in_array(strtolower($parts['scheme']), array('http', 'https'), true)) return '';
	if (isset($parts['user']) || isset($parts['pass'])) return '';
	return $value;
}

function crm_valid_webhook_url($value)
{
	if (!is_string($value) || $value === '' || strlen($value) > 2048) return false;
	$parsed = filter_var($value, FILTER_VALIDATE_URL) ? parse_url($value) : false;
	return is_array($parsed)
		&& isset($parsed['scheme'], $parsed['host'])
		&& strtolower($parsed['scheme']) === 'https'
		&& !isset($parsed['user'])
		&& !isset($parsed['pass'])
		&& !isset($parsed['fragment']);
}

function crm_new_submission_id()
{
	$bytes = random_bytes(16);
	$bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
	$bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);
	$hex = bin2hex($bytes);
	return substr($hex, 0, 8) . '-' . substr($hex, 8, 4) . '-' . substr($hex, 12, 4) . '-'
		. substr($hex, 16, 4) . '-' . substr($hex, 20);
}

function crm_valid_submission_id($value)
{
	return is_string($value)
		&& preg_match('/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i', $value) === 1;
}

function crm_form_submission_id()
{
	$value = isset($_GET['submission_id']) && is_string($_GET['submission_id']) ? trim($_GET['submission_id']) : '';
	return crm_valid_submission_id($value) ? strtolower($value) : crm_new_submission_id();
}

function crm_submission_id_from_post()
{
	$value = crm_post_string('submission_id', 36, '');
	if ($value === '') return crm_new_submission_id();
	if ($value === false || !crm_valid_submission_id($value)) {
		return false;
	}
	return strtolower($value);
}

function crm_require_post()
{
	if (isset($_SERVER['REQUEST_METHOD']) && strtoupper((string)$_SERVER['REQUEST_METHOD']) === 'POST') return;
	header('Allow: POST');
	http_response_code(405);
	header('Content-Type: text/plain; charset=utf-8');
	echo 'Method Not Allowed';
	exit;
}

function crm_request_shape_is_bounded($maxBytes, $maxPostFields)
{
	if (isset($_SERVER['CONTENT_LENGTH'])) {
		$contentLength = filter_var($_SERVER['CONTENT_LENGTH'], FILTER_VALIDATE_INT);
		if ($contentLength === false || $contentLength < 0 || $contentLength > (int)$maxBytes) return false;
	}
	return count($_POST) <= (int)$maxPostFields;
}

function crm_redirect($path, $error = '', $submissionId = '')
{
	$query = array();
	if ($error !== '') $query['error'] = $error;
	if (crm_valid_submission_id($submissionId)) $query['submission_id'] = strtolower($submissionId);
	if (!empty($query)) {
		$separator = strpos($path, '?') === false ? '?' : '&';
		$path .= $separator . http_build_query($query, '', '&', PHP_QUERY_RFC3986);
	}
	header('Location: ' . $path, true, 303);
	exit;
}

function crm_form_return_path($fallback)
{
	$referer = isset($_SERVER['HTTP_REFERER']) && is_string($_SERVER['HTTP_REFERER']) ? $_SERVER['HTTP_REFERER'] : '';
	$host = isset($_SERVER['HTTP_HOST']) && is_string($_SERVER['HTTP_HOST']) ? strtolower($_SERVER['HTTP_HOST']) : '';
	if (strlen($referer) > 2048 || strlen($host) > 255) return $fallback;
	$parts = $referer !== '' ? parse_url($referer) : false;
	if (!is_array($parts) || !isset($parts['host'], $parts['path']) || $host === '') return $fallback;
	$refererHost = strtolower($parts['host']);
	$currentHost = strtolower(preg_replace('/:\d+$/', '', $host));
	if ($refererHost !== $currentHost) return $fallback;
	$path = (string)$parts['path'];
	if ($path === '' || $path[0] !== '/' || strpos($path, '//') === 0 || strpos($path, '/php/send-email') === 0) return $fallback;
	return $path;
}

function crm_form_error_message($error, $lang = 'fr')
{
	$messages = array(
		'validation' => array('fr' => 'Veuillez vérifier les champs du formulaire et réessayer.', 'en' => 'Please check the form fields and try again.'),
		'files' => array('fr' => 'Un fichier joint est invalide ou trop volumineux.', 'en' => 'An attachment is invalid or too large.'),
		'rate' => array('fr' => 'Trop de demandes ont été envoyées. Veuillez réessayer dans quelques minutes.', 'en' => 'Too many requests were sent. Please try again in a few minutes.'),
		'verification' => array('fr' => 'La vérification de sécurité a échoué. Veuillez réessayer.', 'en' => 'Security verification failed. Please try again.'),
		'delivery' => array('fr' => 'La demande n’a pas pu être livrée. Veuillez réessayer ou nous appeler.', 'en' => 'The request could not be delivered. Please try again or call us.'),
	);
	if (!is_string($error) || !isset($messages[$error])) return '';
	$language = $lang === 'en' ? 'en' : 'fr';
	return $messages[$error][$language];
}

function crm_honeypot_triggered()
{
	if (!isset($_POST['phone'])) return false;
	return !is_string($_POST['phone']) || trim($_POST['phone']) !== '';
}

function crm_private_runtime_root($environmentName, $defaultDirectory)
{
	$configured = getenv($environmentName);
	$root = ($configured !== false && trim($configured) !== '')
		? rtrim(trim($configured), DIRECTORY_SEPARATOR)
		: rtrim(sys_get_temp_dir(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $defaultDirectory;

	if (!is_dir($root) && !@mkdir($root, 0700, true)) return false;
	if (!@chmod($root, 0700) || !is_writable($root)) return false;
	$realRoot = realpath($root);
	if ($realRoot === false) return false;
	$permissions = @fileperms($realRoot);
	if ($permissions === false || ($permissions & 0077) !== 0) return false;

	$documentRoot = isset($_SERVER['DOCUMENT_ROOT']) ? realpath($_SERVER['DOCUMENT_ROOT']) : false;
	if ($documentRoot !== false) {
		$rootPrefix = rtrim($realRoot, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR;
		$documentPrefix = rtrim($documentRoot, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR;
		if (strpos($rootPrefix, $documentPrefix) === 0) return false;
	}
	return $realRoot;
}

function crm_request_ip()
{
	$remote = isset($_SERVER['REMOTE_ADDR']) && is_string($_SERVER['REMOTE_ADDR'])
		&& filter_var($_SERVER['REMOTE_ADDR'], FILTER_VALIDATE_IP)
		? $_SERVER['REMOTE_ADDR']
		: 'unknown';
	$configured = getenv('CRM_TRUSTED_PROXY_IPS');
	if ($configured === false || trim($configured) === '' || $remote === 'unknown') return $remote;

	$trusted = array();
	foreach (explode(',', $configured) as $candidate) {
		$candidate = trim($candidate);
		if (filter_var($candidate, FILTER_VALIDATE_IP)) $trusted[$candidate] = true;
	}
	if (!isset($trusted[$remote])) return $remote;

	$forwarded = isset($_SERVER['HTTP_X_FORWARDED_FOR']) && is_string($_SERVER['HTTP_X_FORWARDED_FOR'])
		? $_SERVER['HTTP_X_FORWARDED_FOR']
		: '';
	if ($forwarded === '' || strlen($forwarded) > 1024) return $remote;
	$chain = array_slice(array_map('trim', explode(',', $forwarded)), -20);
	for ($index = count($chain) - 1; $index >= 0; $index--) {
		$candidate = $chain[$index];
		if (!filter_var($candidate, FILTER_VALIDATE_IP)) continue;
		if (isset($trusted[$candidate])) continue;
		return $candidate;
	}
	return $remote;
}

function crm_environment_value($name)
{
	$value = getenv($name);
	return $value === false ? '' : trim((string)$value);
}

function crm_form_is_production()
{
	$explicit = strtolower(crm_environment_value('CRM_FORM_PRODUCTION'));
	if (in_array($explicit, array('1', 'true', 'yes', 'on'), true)) return true;

	$environment = crm_environment_value('CRM_FORM_ENV');
	if ($environment === '') $environment = crm_environment_value('APP_ENV');
	if ($environment === '') $environment = crm_environment_value('ENV');
	return in_array(strtolower($environment), array('prod', 'production'), true);
}

function crm_bounded_credential($name, $maxLength)
{
	$value = crm_environment_value($name);
	if ($value === '') return '';
	if (strlen($value) > (int)$maxLength || preg_match('/^[\x21-\x7E]+$/D', $value) !== 1) return false;
	return $value;
}

function crm_turnstile_site_key()
{
	$siteKey = crm_bounded_credential('TURNSTILE_SITE_KEY', 256);
	if (is_string($siteKey) && $siteKey !== '' && preg_match('/^[A-Za-z0-9_-]+$/D', $siteKey) !== 1) return false;
	return $siteKey;
}

function crm_turnstile_secret_key()
{
	return crm_bounded_credential('TURNSTILE_SECRET_KEY', 512);
}

function crm_turnstile_expected_hostnames()
{
	$configured = crm_environment_value('TURNSTILE_EXPECTED_HOSTNAMES');
	if ($configured === '') return array();
	$hostnames = array();
	foreach (explode(',', $configured) as $rawHostname) {
		$hostname = strtolower(rtrim(trim($rawHostname), '.'));
		if (
			$hostname === ''
			|| strlen($hostname) > 253
			|| preg_match('/^[a-z0-9.-]+$/', $hostname) !== 1
			|| strpos($hostname, '..') !== false
		) return false;
		$hostnames[$hostname] = true;
	}
	return array_keys($hostnames);
}

function crm_turnstile_is_configured()
{
	return crm_environment_value('TURNSTILE_SITE_KEY') !== ''
		|| crm_environment_value('TURNSTILE_SECRET_KEY') !== '';
}

function crm_turnstile_is_required()
{
	return crm_form_is_production() || crm_turnstile_is_configured();
}

function crm_turnstile_action($action)
{
	$action = is_string($action) ? trim($action) : '';
	return strlen($action) <= 32 && preg_match('/^[A-Za-z0-9_-]+$/', $action) === 1 ? $action : '';
}

function crm_turnstile_script_html()
{
	$siteKey = crm_turnstile_site_key();
	if (!is_string($siteKey) || $siteKey === '') return '';
	return '<script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>';
}

function crm_turnstile_widget_html($action)
{
	$siteKey = crm_turnstile_site_key();
	$action = crm_turnstile_action($action);
	if (!is_string($siteKey) || $siteKey === '' || $action === '') return '';
	return '<div class="cf-turnstile" data-sitekey="' . crm_html($siteKey)
		. '" data-action="' . crm_html($action)
		. '" data-response-field-name="cf-turnstile-response"></div>';
}

function crm_rate_limit_redis_configuration()
{
	$url = crm_environment_value('CRM_RATE_LIMIT_REDIS_URL');
	if ($url === '' || strlen($url) > 2048) return false;
	$parts = parse_url($url);
	if (!is_array($parts) || !isset($parts['scheme'], $parts['host'])) return false;
	$scheme = strtolower((string)$parts['scheme']);
	if (!in_array($scheme, array('redis', 'rediss'), true)) return false;
	if (isset($parts['query']) || isset($parts['fragment'])) return false;
	$host = (string)$parts['host'];
	if (
		$host === ''
		|| strlen($host) > 253
		|| (
			filter_var($host, FILTER_VALIDATE_IP) === false
			&& preg_match('/^[A-Za-z0-9.-]+$/D', $host) !== 1
		)
	) return false;
	$port = isset($parts['port']) ? (int)$parts['port'] : 6379;
	if ($port < 1 || $port > 65535) return false;
	$databasePath = isset($parts['path']) ? trim((string)$parts['path'], '/') : '';
	if ($databasePath !== '' && preg_match('/^[0-9]{1,5}$/', $databasePath) !== 1) return false;
	$database = $databasePath === '' ? 0 : (int)$databasePath;
	if ($database > 65535) return false;
	$username = isset($parts['user']) ? rawurldecode((string)$parts['user']) : '';
	$password = isset($parts['pass']) ? rawurldecode((string)$parts['pass']) : '';
	if (strlen($username) > 512 || strlen($password) > 512 || strpos($username, "\0") !== false || strpos($password, "\0") !== false) return false;

	return array(
		'scheme' => $scheme,
		'host' => $host,
		'port' => $port,
		'database' => $database,
		'username' => $username,
		'password' => $password,
	);
}

function crm_form_security_configuration_errors($redisAvailable = null)
{
	$errors = array();
	$siteKey = crm_turnstile_site_key();
	$secretKey = crm_turnstile_secret_key();
	$turnstileConfigured = $siteKey !== '' || $secretKey !== '';
	$turnstileRequired = crm_form_is_production() || $turnstileConfigured;

	if ($turnstileRequired && (!is_string($siteKey) || $siteKey === '')) $errors[] = 'TURNSTILE_SITE_KEY';
	if ($turnstileRequired && (!is_string($secretKey) || $secretKey === '')) $errors[] = 'TURNSTILE_SECRET_KEY';
	if (crm_turnstile_expected_hostnames() === false) $errors[] = 'TURNSTILE_EXPECTED_HOSTNAMES';

	if (crm_form_is_production()) {
		$redisConfiguration = crm_rate_limit_redis_configuration();
		if ($redisConfiguration === false) $errors[] = 'CRM_RATE_LIMIT_REDIS_URL';
		$detectRedisVersion = $redisAvailable === null;
		$redisAvailable = $detectRedisVersion ? class_exists('Redis') : (bool)$redisAvailable;
		if (!$redisAvailable) $errors[] = 'phpredis extension';
		if (
			$detectRedisVersion
			&& $redisAvailable
			&& is_array($redisConfiguration)
			&& $redisConfiguration['scheme'] === 'rediss'
			&& (
				phpversion('redis') === false
				|| version_compare((string)phpversion('redis'), '5.3.0', '<')
			)
		) $errors[] = 'phpredis >= 5.3 for rediss';
	}
	return array_values(array_unique($errors));
}

function crm_require_form_security_configuration()
{
	$errors = crm_form_security_configuration_errors();
	if (empty($errors)) return;
	error_log('Public form security configuration is incomplete: ' . implode(', ', $errors) . '.');
	http_response_code(503);
	header('Content-Type: text/plain; charset=utf-8');
	header('Cache-Control: no-store');
	echo 'Form temporarily unavailable.';
	exit;
}

function crm_open_rate_limit_redis($configuration)
{
	if (!is_array($configuration) || !class_exists('Redis')) return false;
	$redis = null;
	try {
		$redis = new Redis();
		$redisHost = $configuration['host'];
		if ($configuration['scheme'] === 'rediss') {
			$redisHost = strpos($redisHost, ':') !== false ? '[' . $redisHost . ']' : $redisHost;
			$redisHost = 'tls://' . $redisHost;
			$connected = $redis->connect(
				$redisHost,
				(int)$configuration['port'],
				0.75,
				null,
				0,
				0.75,
				array('stream' => array(
					'verify_peer' => true,
					'verify_peer_name' => true,
					'peer_name' => $configuration['host'],
					'allow_self_signed' => false,
				))
			);
		} else {
			$connected = $redis->connect($redisHost, (int)$configuration['port'], 0.75, null, 0, 0.75);
		}
		if (!$connected) {
			throw new RuntimeException('Redis connection failed');
		}
		if (defined('Redis::OPT_READ_TIMEOUT')) $redis->setOption(Redis::OPT_READ_TIMEOUT, 0.75);
		if ($configuration['username'] !== '') {
			if (!$redis->auth(array($configuration['username'], $configuration['password']))) {
				throw new RuntimeException('Redis authentication failed');
			}
		} elseif ($configuration['password'] !== '') {
			if (!$redis->auth($configuration['password'])) throw new RuntimeException('Redis authentication failed');
		}
		if ((int)$configuration['database'] !== 0 && !$redis->select((int)$configuration['database'])) {
			throw new RuntimeException('Redis database selection failed');
		}
		return $redis;
	} catch (Throwable $exception) {
		if (is_object($redis) && method_exists($redis, 'close')) {
			try { $redis->close(); } catch (Throwable $ignored) {}
		}
		return false;
	}
}

function crm_rate_limit_allow_redis($scope, $limit, $windowSeconds, $redis = null)
{
	$ownedClient = false;
	if ($redis === null) {
		$configuration = crm_rate_limit_redis_configuration();
		if ($configuration === false) return null;
		$redis = crm_open_rate_limit_redis($configuration);
		if ($redis === false) return null;
		$ownedClient = true;
	}
	$prefix = crm_environment_value('CRM_RATE_LIMIT_REDIS_PREFIX');
	if ($prefix === '' || strlen($prefix) > 80 || preg_match('/^[A-Za-z0-9:_-]+$/', $prefix) !== 1) {
		$prefix = '3dpreciscan:form-rate';
	}
	$key = $prefix . ':' . hash('sha256', (string)$scope . '|' . crm_request_ip());
	$windowSeconds = max(1, (int)$windowSeconds);
	$limit = max(1, (int)$limit);
	$script = "local current = redis.call('INCR', KEYS[1])\n"
		. "if current == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end\n"
		. "if redis.call('TTL', KEYS[1]) < 0 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end\n"
		. "return current";
	try {
		$count = $redis->eval($script, array($key, (string)$windowSeconds), 1);
		if (!is_int($count) && !is_numeric($count)) return null;
		return (int)$count <= $limit;
	} catch (Throwable $exception) {
		return null;
	} finally {
		if ($ownedClient && is_object($redis) && method_exists($redis, 'close')) {
			try { $redis->close(); } catch (Throwable $ignored) {}
		}
	}
}

function crm_rate_limit_allow($scope, $limit = 6, $windowSeconds = 600)
{
	$redisConfiguration = crm_rate_limit_redis_configuration();
	if ($redisConfiguration !== false && class_exists('Redis')) {
		$redisResult = crm_rate_limit_allow_redis($scope, $limit, $windowSeconds);
		if ($redisResult !== null) return $redisResult;
		error_log('Distributed public form rate limiter is unavailable.');
	}
	if (crm_form_is_production()) {
		error_log('Production public forms require the Redis rate limiter; local file fallback was refused.');
		return false;
	}
	return crm_rate_limit_allow_file($scope, $limit, $windowSeconds);
}

function crm_rate_limit_allow_file($scope, $limit = 6, $windowSeconds = 600)
{
	$root = crm_private_runtime_root('CRM_RATE_LIMIT_DIR', '3dpreciscan-form-rate-limits');
	if ($root === false) return false;

	$ip = crm_request_ip();
	$bucketKey = hash('sha256', (string)$scope . '|' . $ip);
	$filePath = $root . DIRECTORY_SEPARATOR . 'buckets.json';
	$handle = @fopen($filePath, 'c+');
	if ($handle === false) return false;
	@chmod($filePath, 0600);

	$allowed = false;
	if (@flock($handle, LOCK_EX)) {
		$stat = fstat($handle);
		$raw = '';
		if (is_array($stat) && isset($stat['size']) && $stat['size'] > 0 && $stat['size'] <= 1024 * 1024) {
			rewind($handle);
			$raw = (string)stream_get_contents($handle, 1024 * 1024);
		}
		$buckets = $raw !== '' ? json_decode($raw, true) : array();
		if (!is_array($buckets)) $buckets = array();
		$now = time();
		$cutoff = $now - max(1, (int)$windowSeconds);

		foreach ($buckets as $key => $bucket) {
			if (!is_array($bucket) || !isset($bucket['updated']) || (int)$bucket['updated'] < $cutoff) {
				unset($buckets[$key]);
			}
		}

		$current = isset($buckets[$bucketKey]['timestamps']) && is_array($buckets[$bucketKey]['timestamps'])
			? $buckets[$bucketKey]['timestamps']
			: array();
		$current = array_values(array_filter($current, function ($timestamp) use ($cutoff) {
			return is_int($timestamp) && $timestamp >= $cutoff;
		}));
		if (count($current) < max(1, (int)$limit)) {
			$current[] = $now;
			$allowed = true;
		}
		$buckets[$bucketKey] = array('updated' => $now, 'timestamps' => $current);

		if (count($buckets) > 2048) {
			uasort($buckets, function ($left, $right) {
				return (int)$right['updated'] <=> (int)$left['updated'];
			});
			$buckets = array_slice($buckets, 0, 2048, true);
		}

		$encoded = json_encode($buckets);
		if ($encoded === false) {
			$allowed = false;
		} else {
			rewind($handle);
			if (!ftruncate($handle, 0) || fwrite($handle, $encoded) !== strlen($encoded) || !fflush($handle)) {
				$allowed = false;
			}
		}
		flock($handle, LOCK_UN);
	}
	fclose($handle);
	return $allowed;
}

function crm_turnstile_http_post($url, $fields)
{
	if ($url !== 'https://challenges.cloudflare.com/turnstile/v0/siteverify' || !is_array($fields)) return false;
	$encoded = http_build_query($fields, '', '&', PHP_QUERY_RFC3986);
	if (function_exists('curl_init')) {
		$ch = curl_init($url);
		if ($ch === false) return false;
		$responseBody = '';
		$responseTooLarge = false;
		curl_setopt($ch, CURLOPT_POST, true);
		curl_setopt($ch, CURLOPT_HTTPHEADER, array(
			'Content-Type: application/x-www-form-urlencoded',
			'Accept: application/json',
		));
		curl_setopt($ch, CURLOPT_POSTFIELDS, $encoded);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, false);
		curl_setopt($ch, CURLOPT_WRITEFUNCTION, function ($handle, $chunk) use (&$responseBody, &$responseTooLarge) {
			if (strlen($responseBody) + strlen($chunk) > 64 * 1024) {
				$responseTooLarge = true;
				return 0;
			}
			$responseBody .= $chunk;
			return strlen($chunk);
		});
		curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 2);
		curl_setopt($ch, CURLOPT_TIMEOUT, 4);
		curl_setopt($ch, CURLOPT_FOLLOWLOCATION, false);
		curl_setopt($ch, CURLOPT_MAXREDIRS, 0);
		curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, true);
		curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 2);
		curl_setopt($ch, CURLOPT_NOSIGNAL, true);
		if (defined('CURLOPT_PROTOCOLS_STR')) {
			curl_setopt($ch, CURLOPT_PROTOCOLS_STR, 'https');
		} elseif (defined('CURLOPT_PROTOCOLS') && defined('CURLPROTO_HTTPS')) {
			curl_setopt($ch, CURLOPT_PROTOCOLS, CURLPROTO_HTTPS);
		}
		$result = curl_exec($ch);
		$status = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
		curl_close($ch);
		if ($result === false || $responseTooLarge || $status < 200 || $status >= 300) return false;
	} else {
		$context = stream_context_create(array(
			'http' => array(
				'method' => 'POST',
				'header' => "Content-Type: application/x-www-form-urlencoded\r\nAccept: application/json\r\n",
				'content' => $encoded,
				'timeout' => 4,
				'ignore_errors' => true,
				'follow_location' => 0,
				'max_redirects' => 0,
				'protocol_version' => 1.1,
			),
			'ssl' => array('verify_peer' => true, 'verify_peer_name' => true),
		));
		$responseBody = @file_get_contents($url, false, $context, 0, (64 * 1024) + 1);
		$status = 0;
		if (function_exists('http_get_last_response_headers')) {
			$responseHeaders = http_get_last_response_headers();
		} else {
			$localVariables = get_defined_vars();
			$responseHeaders = isset($localVariables['http_response_header']) ? $localVariables['http_response_header'] : array();
		}
		if (!is_array($responseHeaders)) $responseHeaders = array();
		foreach ($responseHeaders as $responseHeader) {
			if (preg_match('/^HTTP\/\S+\s+(\d{3})\b/i', $responseHeader, $matches)) $status = (int)$matches[1];
		}
		if ($responseBody === false || strlen($responseBody) > 64 * 1024 || $status < 200 || $status >= 300) return false;
	}
	$decoded = json_decode($responseBody, true);
	return is_array($decoded) ? $decoded : false;
}

function crm_verify_turnstile_token($token, $expectedAction, $remoteIp = null, $transport = null)
{
	$secretKey = crm_turnstile_secret_key();
	$expectedAction = crm_turnstile_action($expectedAction);
	if (!is_string($secretKey) || $secretKey === '' || $expectedAction === '') return false;
	if (!is_string($token)) return false;
	$token = trim($token);
	if ($token === '' || strlen($token) > 2048 || preg_match('/\s/', $token)) return false;
	$fields = array(
		'secret' => $secretKey,
		'response' => $token,
		'idempotency_key' => crm_new_submission_id(),
	);
	$remoteIp = $remoteIp === null ? crm_request_ip() : $remoteIp;
	if (is_string($remoteIp) && filter_var($remoteIp, FILTER_VALIDATE_IP)) $fields['remoteip'] = $remoteIp;
	$url = 'https://challenges.cloudflare.com/turnstile/v0/siteverify';
	try {
		$result = is_callable($transport) ? call_user_func($transport, $url, $fields) : crm_turnstile_http_post($url, $fields);
	} catch (Throwable $exception) {
		$result = false;
	}
	if (!is_array($result) || !isset($result['success']) || $result['success'] !== true) {
		$errorCodes = is_array($result) && isset($result['error-codes']) && is_array($result['error-codes'])
			? array_filter($result['error-codes'], function ($code) { return is_string($code) && preg_match('/^[A-Za-z0-9_-]{1,80}$/', $code); })
			: array('internal-error');
		error_log('Turnstile validation failed: ' . implode(', ', array_slice($errorCodes, 0, 8)) . '.');
		return false;
	}
	if (!isset($result['action']) || !is_string($result['action']) || !hash_equals($expectedAction, $result['action'])) {
		error_log('Turnstile validation failed: action mismatch.');
		return false;
	}
	$expectedHostnames = crm_turnstile_expected_hostnames();
	if ($expectedHostnames === false) return false;
	if (!empty($expectedHostnames)) {
		$hostname = isset($result['hostname']) && is_string($result['hostname'])
			? strtolower(rtrim($result['hostname'], '.'))
			: '';
		if (!in_array($hostname, $expectedHostnames, true)) {
			error_log('Turnstile validation failed: hostname mismatch.');
			return false;
		}
	}
	return true;
}

function crm_turnstile_submission_is_valid($expectedAction, $transport = null)
{
	if (!crm_turnstile_is_required()) return true;
	$token = isset($_POST['cf-turnstile-response']) ? $_POST['cf-turnstile-response'] : '';
	return crm_verify_turnstile_token($token, $expectedAction, crm_request_ip(), $transport);
}

function crm_mail_from_address()
{
	$configured = getenv('CRM_MAIL_FROM');
	$address = ($configured !== false && trim($configured) !== '') ? trim($configured) : 'website@3dpreciscan.com';
	return crm_valid_email($address) ? $address : false;
}

function crm_send_html_mail($to, $subject, $html, $replyTo)
{
	$from = crm_mail_from_address();
	if ($from === false || !crm_valid_email($replyTo)) return false;
	$headers = 'MIME-Version: 1.0' . "\r\n";
	$headers .= 'Content-Type: text/html; charset=UTF-8' . "\r\n";
	$headers .= 'From: 3D PreciScan <' . $from . '>' . "\r\n";
	$headers .= 'Reply-To: ' . $replyTo . "\r\n";
	return mail($to, $subject, $html, $headers, '-f' . $from);
}

function crm_log_delivery($handler, $submissionId, $mailSent, $crmSent)
{
	$safeHandler = preg_replace('/[^A-Za-z0-9._-]+/', '-', (string)$handler);
	$safeId = preg_replace('/[^a-f0-9-]+/i', '', (string)$submissionId);
	error_log($safeHandler . ': submission=' . $safeId . ' mail=' . ($mailSent ? 'sent' : 'failed') . ' crm=' . ($crmSent ? 'sent' : 'failed'));
}

function crm_post_value($key, $default = '')
{
	$value = crm_post_string($key, 256, $default);
	return $value === false ? $default : $value;
}

function crm_post_raw_value($key, $default = '')
{
	if (!isset($_POST[$key]) || !is_string($_POST[$key])) return $default;
	return trim($_POST[$key]);
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
	if ($posted !== '') return crm_safe_web_url($posted);
	$referer = isset($_SERVER['HTTP_REFERER']) ? $_SERVER['HTTP_REFERER'] : '';
	return crm_safe_web_url($referer);
}

function crm_referrer_url()
{
	$posted = crm_post_raw_value('referrer_url');
	if ($posted !== '') return crm_safe_web_url($posted);
	return isset($_SERVER['HTTP_REFERER']) ? crm_safe_web_url($_SERVER['HTTP_REFERER']) : '';
}

function crm_sms_consent_payload($formName)
{
	$raw = strtolower(crm_post_raw_value('sms_consent'));
	$withdrawal = strtolower(crm_post_raw_value('sms_consent_action'));
	$withdrawn = in_array($withdrawal, array('withdraw', 'withdrawn', 'opt_out'), true);
	$granted = !$withdrawn && in_array($raw, array('1', 'true', 'yes', 'on', 'accepted'), true);
	$payload = array(
		'method' => $granted ? 'explicit_checkbox' : ($withdrawn ? 'explicit_withdrawal' : 'not_provided'),
		'captured_at' => gmdate('c'),
		'form' => substr(preg_replace('/[^A-Za-z0-9._-]+/', '-', (string)$formName), 0, 80),
		'text' => "J’accepte de recevoir des messages texte de 3D PreciScan au numéro fourni au sujet de ma demande. La fréquence varie. Des frais de messagerie peuvent s’appliquer. Mon consentement n’est pas une condition de service. Répondez STOP pour vous désabonner.",
	);
	if ($granted) $payload['sms'] = true;
	if ($withdrawn) $payload['sms'] = false;
	return $payload;
}

function crm_secure_upload_root()
{
	return crm_private_runtime_root('CRM_UPLOAD_TMP_DIR', '3dpreciscan-quote-uploads');
}

function crm_detect_safe_upload_type($path)
{
	$size = @filesize($path);
	if ($size === false || $size <= 0) return false;
	$detectedMime = false;
	if (function_exists('finfo_open')) {
		$fileInfo = @finfo_open(FILEINFO_MIME_TYPE);
		if ($fileInfo !== false) {
			$detectedMime = @finfo_file($fileInfo, $path);
		}
	}

	$imageInfo = @getimagesize($path);
	if (is_array($imageInfo) && isset($imageInfo[0], $imageInfo[1], $imageInfo[2])) {
		if ((int)$imageInfo[0] <= 0 || (int)$imageInfo[1] <= 0 || ((int)$imageInfo[0] * (int)$imageInfo[1]) > 40000000) {
			return false;
		}
		$imageTypes = array(
			1 => array('mime' => 'image/gif', 'extension' => 'gif'),
			2 => array('mime' => 'image/jpeg', 'extension' => 'jpg'),
			3 => array('mime' => 'image/png', 'extension' => 'png'),
			18 => array('mime' => 'image/webp', 'extension' => 'webp'),
		);
		if (isset($imageTypes[(int)$imageInfo[2]])) {
			$type = $imageTypes[(int)$imageInfo[2]];
			if ($detectedMime !== false && $detectedMime !== $type['mime']) return false;
			return $type;
		}
	}

	$head = @file_get_contents($path, false, null, 0, 4096);
	if ($head === false) return false;
	$tailOffset = max(0, $size - 2048);
	$tail = @file_get_contents($path, false, null, $tailOffset, 2048);
	if (
		preg_match('/^%PDF-[12]\.[0-9]/', $head)
		&& $tail !== false
		&& strpos($tail, '%%EOF') !== false
		&& ($detectedMime === false || $detectedMime === 'application/pdf')
	) {
		return array('mime' => 'application/pdf', 'extension' => 'pdf');
	}
	if (
		preg_match('/^\s*solid(?:\s|$)/i', $head)
		&& stripos($head, 'facet') !== false
		&& $tail !== false
		&& preg_match('/endsolid\s*$/i', $tail)
		&& ($detectedMime === false || in_array($detectedMime, array('application/octet-stream', 'application/sla', 'model/stl', 'model/x.stl-ascii', 'text/plain'), true))
	) {
		return array('mime' => 'model/stl', 'extension' => 'stl');
	}
	if ($size >= 84) {
		$binaryHeader = @file_get_contents($path, false, null, 0, 84);
		if ($binaryHeader !== false && strlen($binaryHeader) === 84) {
			$count = unpack('Vtriangles', substr($binaryHeader, 80, 4));
			$triangles = is_array($count) && isset($count['triangles']) ? (int)$count['triangles'] : -1;
			if (
				$triangles >= 0
				&& 84 + ($triangles * 50) === $size
				&& ($detectedMime === false || in_array($detectedMime, array('application/octet-stream', 'application/sla', 'model/stl', 'model/x.stl-binary'), true))
			) {
				return array('mime' => 'model/stl', 'extension' => 'stl');
			}
		}
	}
	return false;
}

function crm_store_quote_uploads($upload, &$emergencyCleanupFiles = null)
{
	$result = array('files' => array(), 'errors' => array());
	$emergencyCleanupFiles = array();
	if (!is_array($upload) || !isset($upload['name'])) return $result;
	foreach (array('name', 'tmp_name', 'error', 'size') as $requiredKey) {
		if (!isset($upload[$requiredKey])) {
			$result['errors'][] = 'Métadonnées de fichier incomplètes.';
			return $result;
		}
	}

	$names = is_array($upload['name']) ? $upload['name'] : array($upload['name']);
	$tmpNames = is_array($upload['tmp_name']) ? $upload['tmp_name'] : array($upload['tmp_name']);
	$errors = is_array($upload['error']) ? $upload['error'] : array($upload['error']);
	$sizes = is_array($upload['size']) ? $upload['size'] : array($upload['size']);
	if (count($names) !== count($tmpNames) || count($names) !== count($errors) || count($names) !== count($sizes)) {
		$result['errors'][] = 'Métadonnées de fichier incohérentes.';
		return $result;
	}
	$presentCount = 0;
	foreach ($names as $name) {
		if (!is_string($name)) {
			$result['errors'][] = 'Nom de fichier invalide.';
			return $result;
		}
		if (trim($name) !== '') $presentCount++;
	}
	if ($presentCount > 5) {
		$result['errors'][] = 'Maximum de 5 fichiers.';
		return $result;
	}

	$root = crm_secure_upload_root();
	if ($root === false && $presentCount > 0) {
		$result['errors'][] = 'Le stockage temporaire sécurisé est indisponible.';
		return $result;
	}

	$totalBytes = 0;
	foreach ($names as $index => $originalName) {
		if (trim($originalName) === '') continue;
		if (!is_string($tmpNames[$index]) || !is_scalar($errors[$index]) || !is_scalar($sizes[$index])) {
			$result['errors'][] = 'Métadonnées de fichier invalides.';
			continue;
		}
		$error = isset($errors[$index]) ? (int)$errors[$index] : UPLOAD_ERR_NO_FILE;
		if ($error !== UPLOAD_ERR_OK) {
			$result['errors'][] = 'Un fichier joint n\'a pas pu être téléversé.';
			continue;
		}
		$tmpName = isset($tmpNames[$index]) ? $tmpNames[$index] : '';
		$declaredSize = isset($sizes[$index]) ? (int)$sizes[$index] : 0;
		$actualSize = @filesize($tmpName);
		if (!is_uploaded_file($tmpName) || $actualSize === false || $actualSize !== $declaredSize) {
			$result['errors'][] = 'Fichier joint invalide.';
			continue;
		}
		if ($actualSize > 8 * 1024 * 1024) {
			$result['errors'][] = 'Chaque fichier doit faire au plus 8 Mo.';
			continue;
		}
		$totalBytes += $actualSize;
		if ($totalBytes > 15 * 1024 * 1024) {
			$result['errors'][] = 'La taille totale des fichiers dépasse 15 Mo.';
			continue;
		}
		$type = crm_detect_safe_upload_type($tmpName);
		if ($type === false) {
			$result['errors'][] = 'Type de fichier non autorisé.';
			continue;
		}

		try {
			$generatedName = bin2hex(random_bytes(16)) . '.' . $type['extension'];
		} catch (Throwable $exception) {
			$result['errors'][] = 'Impossible de générer un nom de fichier sécurisé.';
			continue;
		}
		$targetPath = $root . DIRECTORY_SEPARATOR . $generatedName;
		if (!move_uploaded_file($tmpName, $targetPath)) {
			$result['errors'][] = 'Impossible de stocker un fichier joint.';
			continue;
		}
		$emergencyCleanupFiles[] = array('path' => $targetPath);
		@chmod($targetPath, 0600);
		$displayBase = pathinfo(basename((string)$originalName), PATHINFO_FILENAME);
		$displayBase = preg_replace('/[^A-Za-z0-9_-]+/', '-', $displayBase);
		$displayBase = substr(trim($displayBase, '-'), 0, 100);
		if ($displayBase === '') $displayBase = 'piece-jointe';
		$displayName = $displayBase . '.' . $type['extension'];
		$result['files'][] = array(
			'path' => $targetPath,
			'name' => $displayName,
			'mime' => $type['mime'],
			'size' => $actualSize,
		);
		$emergencyCleanupFiles = $result['files'];
	}
	return $result;
}

function crm_cleanup_uploads($files)
{
	$root = crm_secure_upload_root();
	if ($root === false || !is_array($files)) return;
	$rootPrefix = rtrim($root, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR;
	foreach ($files as $file) {
		$path = is_array($file) && isset($file['path']) ? realpath($file['path']) : false;
		if ($path !== false && strpos($path, $rootPrefix) === 0 && is_file($path)) @unlink($path);
	}
}

function crm_send_lead_webhook($payload)
{
	$configuredUrl = getenv('CRM_WEBHOOK_URL');
	$crmWebhookUrl = ($configuredUrl !== false) ? trim($configuredUrl) : '';
	if ($crmWebhookUrl === '') {
		error_log('crm_send_lead_webhook: CRM_WEBHOOK_URL is not configured; CRM relay skipped.');
		return false;
	}
	if (!crm_valid_webhook_url($crmWebhookUrl)) {
		error_log('crm_send_lead_webhook: CRM_WEBHOOK_URL is invalid.');
		return false;
	}
	$configuredSecret = getenv('CRM_WEBHOOK_SECRET');
	$crmWebhookSecret = $configuredSecret === false ? '' : trim($configuredSecret);
	if ($crmWebhookSecret === '') {
		error_log('crm_send_lead_webhook: CRM_WEBHOOK_SECRET is not configured; CRM relay skipped.');
		return false;
	}

	$json = json_encode($payload, JSON_UNESCAPED_UNICODE);
	if ($json === false) {
		error_log('crm_send_lead_webhook: JSON encoding failed.');
		return false;
	}
	if (strlen($json) > 128 * 1024) {
		error_log('crm_send_lead_webhook: payload exceeds 128 KiB.');
		return false;
	}

	$timestamp = (string)time();
	$signature = hash_hmac('sha256', $timestamp . '.' . $json, $crmWebhookSecret);
	$headers = array(
		'Content-Type: application/json',
		'Accept: application/json',
		'X-CRM-Webhook-Timestamp: ' . $timestamp,
		'X-CRM-Webhook-Signature: sha256=' . $signature,
	);

	if (function_exists('curl_init')) {
		$ch = curl_init($crmWebhookUrl);
		if ($ch === false) return false;
		curl_setopt($ch, CURLOPT_POST, true);
		curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
		curl_setopt($ch, CURLOPT_POSTFIELDS, $json);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, false);
		curl_setopt($ch, CURLOPT_WRITEFUNCTION, function ($handle, $data) {
			return strlen($data);
		});
		curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 3);
		curl_setopt($ch, CURLOPT_TIMEOUT, 5);
		curl_setopt($ch, CURLOPT_FOLLOWLOCATION, false);
		curl_setopt($ch, CURLOPT_MAXREDIRS, 0);
		curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, true);
		curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 2);
		curl_setopt($ch, CURLOPT_NOSIGNAL, true);
		if (defined('CURLOPT_PROTOCOLS_STR')) {
			curl_setopt($ch, CURLOPT_PROTOCOLS_STR, 'https');
		} elseif (defined('CURLOPT_PROTOCOLS') && defined('CURLPROTO_HTTPS')) {
			curl_setopt($ch, CURLOPT_PROTOCOLS, CURLPROTO_HTTPS);
		}
		$response = curl_exec($ch);
		$status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
		curl_close($ch);

		if ($response === false || $status < 200 || $status >= 300) {
			error_log('crm_send_lead_webhook: delivery failed with HTTP ' . (int)$status . '.');
			return false;
		}
		return true;
	}

	$context = stream_context_create(array(
		'http' => array(
				'method' => 'POST',
				'header' => implode("\r\n", $headers),
				'content' => $json,
				'timeout' => 5,
				'ignore_errors' => true,
				'follow_location' => 0,
				'max_redirects' => 0,
				'protocol_version' => 1.1,
			),
		'ssl' => array(
				'verify_peer' => true,
				'verify_peer_name' => true,
			),
	));
	$response = @file_get_contents($crmWebhookUrl, false, $context, 0, 4096);
	$status = 0;
	if (function_exists('http_get_last_response_headers')) {
		$responseHeaders = http_get_last_response_headers();
	} else {
		$localVariables = get_defined_vars();
		$responseHeaders = isset($localVariables['http_response_header']) ? $localVariables['http_response_header'] : array();
	}
	if (is_array($responseHeaders)) {
		foreach ($responseHeaders as $responseHeader) {
			if (preg_match('/^HTTP\/\S+\s+(\d{3})\b/i', $responseHeader, $matches)) $status = (int)$matches[1];
		}
	}
	if ($response === false || $status < 200 || $status >= 300) {
		error_log('crm_send_lead_webhook: stream delivery failed with HTTP ' . $status . '.');
		return false;
	}
	return true;
}
