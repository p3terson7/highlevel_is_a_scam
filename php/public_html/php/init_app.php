<?php
session_start();

function dossier_maitre()
{
	$url = '/admcl';
	return $url;
}


require($_SERVER['DOCUMENT_ROOT'] . dossier_maitre() . '/php/config.php');

date_default_timezone_set('America/Toronto');

require($_SERVER['DOCUMENT_ROOT'] . dossier_maitre() . '/php/fonctions.bd.php');
require($_SERVER['DOCUMENT_ROOT'] . dossier_maitre() . '/php/fonctions.php');
require($_SERVER['DOCUMENT_ROOT'] . dossier_maitre() . '/php/fonctions.custom.php');


require($_SERVER['DOCUMENT_ROOT'] . '/php/fonctions_contenu.php');

$bdd = connect_bd();

$domain = $_SERVER['HTTP_HOST'];

$page_cours_full= $_SERVER['REQUEST_URI'];
$page_cours = $_SERVER['SCRIPT_NAME'];

$auj = date('Y-m-d');


$prefix_session = prefix_session();

//erreurs

if(isset($_SERVER['HTTP_HOST']))
	$add = $_SERVER['HTTP_HOST'];
else
	$add = '';
if(strpos($add, 'dev') !== false)
	$_SESSION[$prefix_session . '_erreur'] = 1;

if (isset($_GET['erreur']) AND $_GET['erreur'] == 1)
	$_SESSION[$prefix_session . '_erreur'] = 1;
elseif(isset($_GET['erreur']) AND $_GET['erreur'] == 0)
	$_SESSION[$prefix_session . '_erreur'] = 0;

if(!isset($_SESSION[$prefix_session . '_erreur']))
	$_SESSION[$prefix_session . '_erreur'] = 0;

if ($_SESSION[$prefix_session . '_erreur'] == 1)
{
	ini_set('display_errors', 1);
	error_reporting(E_ALL);

}
else
{
	error_reporting(0);
	set_error_handler('myErrorHandler');
	register_shutdown_function('fatalErrorShutdownHandler');
}


//langue

if(!isset($_SESSION[$prefix_session . '_langue']) AND isset($_SERVER['HTTP_ACCEPT_LANGUAGE']))
	$_SESSION[$prefix_session . '_langue'] = substr($_SERVER['HTTP_ACCEPT_LANGUAGE'], 0, 2);
else
	$_SESSION[$prefix_session . '_langue'] = 'fr';
