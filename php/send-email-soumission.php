<?php
error_reporting(E_ALL);
ini_set('display_errors', 0);
ini_set('log_errors', 1);

include ('fonctions.php');

// Honeypot anti-spam (Campo oculto 'phone')
if(isset($_POST['phone']) AND $_POST['phone'] == '')
{
	$fichiers = array();
	$path = "/documents/";
	$lang = isset($_POST['lang']) ? nettoyage($_POST['lang']) : 'fr';

	// 1. Recolección y Sanitización de Campos Requeridos (*)
	$type_client = isset($_POST['type_client']) ? nettoyage($_POST['type_client']) : 'Non spécifié';
	$sNom        = isset($_POST['sNom']) ? trim(nettoyage($_POST['sNom'])) : '';
	$sTel        = isset($_POST['sTel']) ? trim(nettoyage($_POST['sTel'])) : '';
	$sCourriel   = isset($_POST['sCourriel']) ? trim(nettoyage($_POST['sCourriel'])) : '';
	$hauteur     = isset($_POST['hauteur']) ? trim(nettoyage($_POST['hauteur'])) : '';
	$largeur     = isset($_POST['largeur']) ? trim(nettoyage($_POST['largeur'])) : '';
	$longueur    = isset($_POST['longueur']) ? trim(nettoyage($_POST['longueur'])) : '';
	
	// Campos Opcionales
	$autres      = isset($_POST['autres']) ? trim(nettoyage($_POST['autres'])) : 'Aucune';
	$sDelai      = isset($_POST['sDelai']) ? trim(nettoyage($_POST['sDelai'])) : 'Non spécifié';
	$urgent      = isset($_POST['urgent']) ? nettoyage($_POST['urgent']) : 'no';
	$info        = isset($_POST['info']) ? trim(nettoyage($_POST['info'])) : '';

	// 2. SISTEMA DE VALIDACIONES ESTRICTAS (Lado del Servidor)
	$erreurs = array();

	if (empty($sNom))       $erreurs[] = "Le nom est obligatoire.";
	if (empty($sTel))       $erreurs[] = "Le numéro de téléphone est obligatoire.";
	if (empty($hauteur))   $erreurs[] = "La hauteur est obligatoire.";
	if (empty($largeur))   $erreurs[] = "La largeur est obligatoire.";
	if (empty($longueur))  $erreurs[] = "La longueur est obligatoire.";
	
	if (empty($sCourriel)) {
		$erreurs[] = "L'adresse courriel est obligatoire.";
	} elseif (!filter_var($sCourriel, FILTER_VALIDATE_EMAIL)) {
		$erreurs[] = "L'adresse courriel n'est pas valide.";
	}

	// Si hay errores de validación, registramos en el log y redirigimos de inmediato
	if (!empty($erreurs)) {
		error_log('send-email-soumission.php: Erreurs de validation: ' . implode(', ', $erreurs));
		if($lang == "en") {
			header('Location: /en/quote?error=validation');
		} else {
			header('Location: /soumission?error=validation');
		}
		exit;
	}

	// 3. Mapeo Correcto de Checkboxes (Acorde a los "name" de tu HTML)
	$autre_service = array();
	if(isset($_POST['Scan3d']))         $autre_service[] = "Scan 3D";
	if(isset($_POST['Rec3d']))          $autre_service[] = "Reconstruction 3D";
	if(isset($_POST['R_ing3d2d']))      $autre_service[] = "Rétro-ingénérie (3D & 2D)";
	if(isset($_POST['Inspection']))     $autre_service[] = "Inspection 3D";
	if(isset($_POST['Metrologie']))     $autre_service[] = "Métrologie industrielle";
	if(isset($_POST['Concep2d3d']))     $autre_service[] = "Design (3D & 2D)";
	if(isset($_POST['Modelisation3D'])) $autre_service[] = "Modeling"; 
	if(isset($_POST['Ingenerie']))      $autre_service[] = "Ingénierie";
	if(isset($_POST['Imp3d']))          $autre_service[] = "Impression 3D";
	if(isset($_POST['Simul3D']))        $autre_service[] = "Simulation 3D";
	if(isset($_POST['Aucun']))          $autre_service[] = "Je ne sais pas";

	$autres_services = !empty($autre_service) ? implode(', ', $autre_service) : 'Aucun service sélectionné';

	// 4. CONSTRUCCIÓN DEL TEMPLATE HTML ELEGANTE Y MODERNO
	$message = "
	<html>
	<head>
		<meta charset='UTF-8'>
	</head>
	<body style='background-color: #f4f6f9; padding: 20px; font-family: Arial, sans-serif; -webkit-font-smoothing: antialiased;'>
		<div style='max-width: 650px; margin: 0 auto; background-color: #ffffff; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);'>
			
			<div style='background-color: #0f172a; padding: 30px; text-align: center; border-bottom: 4px solid #3b82f6;'>
				<h2 style='color: #ffffff; margin: 0; font-size: 22px; font-weight: 700; letter-spacing: 0.5px; text-transform: uppercase;'>3D PreciScan</h2>
				<p style='color: #94a3b8; margin: 5px 0 0 0; font-size: 14px;'>Nouvelle demande de soumission reçue</p>
			</div>

			<div style='padding: 30px;'>
				
				" . ($urgent === 'yes' ? "
				<div style='background-color: #fef2f2; border-left: 4px solid #ef4444; color: #991b1b; padding: 12px 15px; border-radius: 0 6px 6px 0; margin-bottom: 25px; font-weight: bold; font-size: 14px;'>
					⚠️ Demande signalée comme URGENTE
				</div>
				" : "") . "

				<p style='margin-top: 0; font-size: 15px; color: #334155;'>Bonjour,</p>
				<p style='color: #64748b; font-size: 14px; margin-bottom: 25px;'>Un client vient de soumettre une demande via el formulario del sitio web. Aquí están los detalles recibidos :</p>

				<h3 style='font-size: 14px; text-transform: uppercase; color: #3b82f6; border-bottom: 1px solid #e2e8f0; padding-bottom: 6px; margin-top: 0; margin-bottom: 15px; letter-spacing: 0.5px;'>1. Informations de Contact</h3>
				<table width='100%' border='0' cellspacing='0' cellpadding='0' style='margin-bottom: 25px; font-size: 14px; color: #334155;'>
					<tr>
						<td width='35%' style='padding: 8px 0; font-weight: bold; color: #64748b;'>Secteur d'activité :</td>
						<td style='padding: 8px 0;'>$type_client</td>
					</tr>
					<tr>
						<td style='padding: 8px 0; font-weight: bold; color: #64748b;'>Nom / Entreprise :</td>
						<td style='padding: 8px 0; font-weight: bold; color: #0f172a;'>$sNom</td>
					</tr>
					<tr>
						<td style='padding: 8px 0; font-weight: bold; color: #64748b;'>Téléphone :</td>
						<td style='padding: 8px 0;'><a href='tel:$sTel' style='color: #0f172a; text-decoration: none;'>$sTel</a></td>
					</tr>
					<tr>
						<td style='padding: 8px 0; font-weight: bold; color: #64748b;'>Courriel :</td>
						<td style='padding: 8px 0;'><a href='mailto:$sCourriel' style='color: #3b82f6; text-decoration: none;'>$sCourriel</a></td>
					</tr>
				</table>

				<h3 style='font-size: 14px; text-transform: uppercase; color: #3b82f6; border-bottom: 1px solid #e2e8f0; padding-bottom: 6px; margin-bottom: 15px; letter-spacing: 0.5px;'>2. Services Requis & Délais</h3>
				<table width='100%' border='0' cellspacing='0' cellpadding='0' style='margin-bottom: 25px; font-size: 14px; color: #334155;'>
					<tr>
						<td width='35%' style='padding: 8px 0; font-weight: bold; color: #64748b; valign: top;'>Services sélectionnés :</td>
						<td style='padding: 8px 0; color: #0f172a; font-weight: 500;'>$autres_services</td>
					</tr>
					<tr>
						<td style='padding: 8px 0; font-weight: bold; color: #64748b;'>Délai souhaité :</td>
						<td style='padding: 8px 0; color: #1e40af; font-weight: bold;'>$sDelai</td>
					</tr>
				</table>

				<h3 style='font-size: 14px; text-transform: uppercase; color: #3b82f6; border-bottom: 1px solid #e2e8f0; padding-bottom: 6px; margin-bottom: 15px; letter-spacing: 0.5px;'>3. Dimensions de l'Objet</h3>
				<div style='background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 15px; margin-bottom: 25px;'>
					<table width='100%' border='0' cellspacing='0' cellpadding='0' style='font-size: 13px; color: #475569; text-align: center;'>
						<tr>
							<td style='padding: 5px; border-right: 1px solid #e2e8f0;'><strong>Hauteur</strong><br><span style='font-size: 15px; color: #0f172a; font-weight: bold;'>$hauteur</span></td>
							<td style='padding: 5px; border-right: 1px solid #e2e8f0;'><strong>Largeur</strong><br><span style='font-size: 15px; color: #0f172a; font-weight: bold;'>$largeur</span></td>
							<td style='padding: 5px; border-right: 1px solid #e2e8f0;'><strong>Longueur</strong><br><span style='font-size: 15px; color: #0f172a; font-weight: bold;'>$longueur</span></td>
							<td style='padding: 5px;'><strong>Autres</strong><br><span style='font-size: 15px; color: #0f172a; font-weight: bold;'>$autres</span></td>
						</tr>
					</table>
				</div>

				" . (!empty($info) ? "
				<h3 style='font-size: 14px; text-transform: uppercase; color: #3b82f6; border-bottom: 1px solid #e2e8f0; padding-bottom: 6px; margin-bottom: 12px; letter-spacing: 0.5px;'>4. Informations Additionnelles</h3>
				<div style='background-color: #f1f5f9; padding: 15px; border-radius: 6px; font-size: 14px; color: #334155; line-height: 1.5; font-style: italic; white-space: pre-wrap;'>
					$info
				</div>
				" : "") . "

			</div>

			<div style='background-color: #f8fafc; padding: 20px 30px; text-align: center; font-size: 12px; color: #94a3b8; border-top: 1px solid #e2e8f0;'>
				Ceci est une notification automatique. Répondre à ce courriel écrira directement à l'adresse du client.
			</div>
		</div>
	</body>
	</html>";

	// 5. GESTIÓN Y LIMPIEZA DE ARCHIVOS ADJUNTOS MÚLTIPLES (images[])
	if (isset($_FILES['images']['name'][0]) && !empty($_FILES['images']['name'][0])) {
		$countfiles = count($_FILES['images']['name']);
		for($i=0; $i<$countfiles; $i++){
			$filename = basename($_FILES['images']['name'][$i]);
			
			// Sanitización básica del nombre para evitar ataques de inyección de directorios
			$filename = preg_replace("/[^A-Za-z0-9\.\-_]/", '', $filename);
			
			$target_file = '..' . $path . $filename;
			if(move_uploaded_file($_FILES['images']['tmp_name'][$i], $target_file)){
				$fichiers[] = $filename;
			}
		}
	}

	$to = 'fabien.lagier@3dpreciscan.com, dacampos@publissoft.ca';
	$boundary = "-----=" . md5(uniqid(rand()));
	$subject = '3DPreciscan - Formulaire soumission';
	
	$header = 'From:<fabien.lagier@3dpreciscan.com>' . "\r\n";
	$header .= 'Reply-To:<' . $sCourriel . '>' . "\r\n";
	$header .= "MIME-Version: 1.0\r\n";
	$header .= "Content-Type: multipart/mixed; boundary=\"$boundary\"\r\n\r\n";

	$nmessage = "--" . $boundary . "\r\n";
	$nmessage .= "Content-type:text/html; charset=UTF-8\r\n";
	$nmessage .= "Content-Transfer-Encoding: 7bit\r\n\r\n";
	$nmessage .= $message . "\r\n\r\n";

	foreach($fichiers as $v)
	{
		$file_path = '..' . $path . $v;
		if (file_exists($file_path)) {
			$attachement = chunk_split(base64_encode(file_get_contents($file_path)));
			$contenttype = mime_content_type($file_path);
			
			$nmessage .= "--" . $boundary . "\r\n";
			$nmessage .= "Content-Type: " . $contenttype . "; name=\"" . $v . "\"\r\n";
			$nmessage .= "Content-Transfer-Encoding: base64\r\n";
			$nmessage .= "Content-Disposition: attachment; filename=\"" . $v . "\"\r\n\r\n";
			$nmessage .= $attachement . "\r\n\r\n";
		}
	}
	$nmessage .= "--" . $boundary . "--";

	$result = mail($to, $subject, $nmessage, $header, '-f ' . $to);
	
	if($result) {
		error_log('send-email-soumission.php: mail() OK -> ' . $to);
	} else {
		error_log('send-email-soumission.php: mail() FAILED -> ' . $to);
	}

	crm_send_lead_webhook(array(
		'source_page_url' => crm_source_page_url(),
		'referrer' => crm_referrer_url(),
		'lead' => array(
			'full_name' => $sNom,
			'phone' => $sTel,
			'email' => $sCourriel,
		),
		'form_answers' => array(
			'type_client' => $type_client,
			'full_name' => $sNom,
			'phone' => $sTel,
			'email' => $sCourriel,
			'hauteur' => $hauteur,
			'largeur' => $largeur,
			'longueur' => $longueur,
			'autres_dimensions' => $autres,
			'delai_souhaite' => $sDelai,
			'urgent' => $urgent,
			'services' => $autre_service,
			'services_summary' => $autres_services,
			'informations_additionnelles' => $info,
			'fichiers_joints' => $fichiers,
			'lang' => $lang,
			'source_page_url' => crm_source_page_url(),
			'referrer_url' => crm_referrer_url(),
		),
		'tracking' => crm_tracking_payload(),
	));

	// Limpieza estricta: eliminamos los archivos subidos del servidor tras enviarlos por correo
	foreach($fichiers as $v)
	{
		$del_path = $_SERVER['DOCUMENT_ROOT'] . $path . $v;
		if (file_exists($del_path)) {
			unlink($del_path);
		}
	}

	// Redirección final controlada por idioma
	if($lang == "fr") {
		header('Location: /merci');
	} elseif($lang == "en") {
		header('Location: /en/thanks');
	} else {
		header('Location: /merci');
	}
	exit;
}
else
{
	error_log('send-email-soumission.php: honeypot "phone" con valor detectado.');
	header('Location: /');
	exit;
}
?>