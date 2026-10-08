<?php
	include ("../includes/tophead.php");
	$page = 'services';
	$merciCardTitle = 'Votre demande a bien été reçue.';
	$merciMessage = 'Merci de votre intérêt pour nos services. Un membre de notre équipe communiquera avec vous dans un délai de 12 à 24 heures ouvrables.';
	$merciSecondaryLabel = 'Découvrir nos services';
	$merciSecondaryHref = '/services';
?>

<meta name="robots" content="noindex,follow">
<title>Demande reçue | 3D PreciScan</title>

<?php
	include "../includes/head.php";
	echo '<link rel="stylesheet" href="/php/landing-page-fixes.css?v=20260717">';
	include('../includes/google.php');
?>

<body>
	<noscript>
		<iframe src="https://www.googletagmanager.com/ns.html?id=GTM-NBH3JPT" height="0" width="0" style="display:none;visibility:hidden"></iframe>
	</noscript>

	<?php
		include('../includes/header.php');
		include __DIR__ . '/merci-content.php';
		include "../includes/footer.php";
	?>