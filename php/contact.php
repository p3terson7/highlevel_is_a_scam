<?php
	require_once __DIR__ . '/fonctions.php';
	include ("../includes/tophead.php");
	$page = 'contact';
	$formSubmissionId = crm_form_submission_id();
	$formError = crm_form_error_message(isset($_GET['error']) && is_string($_GET['error']) ? $_GET['error'] : '', 'fr');
	//sitemap
?>

	<link rel="canonical" href="https://3dpreciscan.com/contactez-nous">
	
	<title>
		Contactez-nous - 819 313-1152 - 3D PreciScan - Drummondville
	</title>
					
	<meta name="description" content="Vous avez d'une pièce et/ou objet rapidement ou qui est discontinué ? 3D PreciScan à la solution grâce à notre service de Scan 3D. Appelez-nous !">
					
	<meta name="keywords" content="information, contact, support, courriel, téléphone, formulaire contact, scan 3d, numérisation 3d, fea, reconstruction 3d, conception 3d, laser tracker, impression 3d, ingénierie, inspection 3d, métrologie industrielle,  modélisation 3d, réetro-ingénierie, numérisation 3d, cad">
	
	<!--SEO-->
	<!--meta property="og:title" content="Boutique artisanat en ligne" />
	<meta property="og:description" content="Découvrez une panoplie de produits créés par des artisans du Québec." /--> 
	<meta property="og:site_name"  content="3D PresiScan Inc." />
	<meta property="og:locale"  content="fr_CA" />
	<meta property="og:type" content="website" />
	<meta property="og:url" content="https://3dpreciscan.com/contactez-nous" />
	<!--meta property="og:image" content="https://creationsdici.ca/images/creations-dici/banner_rs1.jpg" /-->

	<?php
		include "../includes/head.php";
		echo crm_turnstile_script_html();
	?>
	
	<script defer src="https://maps.googleapis.com/maps/api/js?key=AIzaSyB1xmzhC6iWC9M-npa404Vh3sUXTIC8l1o&callback=initMap&libraries=&v=weekly&channel=2" async></script>
	<script defer>
	function initMap() {
		// The location of Uluru
		const uluru = { lat: 45.8938484, lng: -72.5364503 };
		// The map, centered at Uluru
		const map = new google.maps.Map(document.getElementById("map"), {
			zoom: 12,
			center: uluru,
			mapTypeControl: true,  		
		});

		var iconMap = {url:"/images/3dpreciscan/cropped-LOGOTYPE_SANS_FOND.png",
			scaledSize: new google.maps.Size(43, 50),
		};
		// The marker, positioned at Uluru
		const marker = new google.maps.Marker({
		  position: uluru,
		  map: map,
		  icon: iconMap,
		//   mapTypeId: google.maps.MapTypeId.HYBRID,
		});
		marker.setMap(map);
		// map.setMapTypeId(google.maps.MapTypeId.HYBRID);
	  }
	</script>
	
	
	
	<?php
		include('../includes/shemas/contact-fr.php');
		include('../includes/google.php');
	?>


	<body itemscope itemtype="http://schema.org/WebPage">
		<noscript>
			<iframe src="https://www.googletagmanager.com/ns.html?id=GTM-NBH3JPT" height="0" width="0" style="display:none;visibility:hidden"></iframe>
		</noscript>
		<meta itemprop="name" content="3D PresiScan Inc. - Contactez-nous"/>
		<meta itemprop="description" content="Besoin d'information sur nos services ? Appelez-nous au 819 313-1152 ou contactez-nous via notre formulaire de contact directement en ligne."/>
		<!--meta itemprop="images" content="information, contact, support"/-->
		<meta itemprop="keywords" content="information, contact, support, courriel, téléphone, formulaire contact, scan 3d, numérisation 3d, fea, reconstruction 3d, conception 3d, laser tracker, impression 3d, ingénierie, inspection 3d, métrologie industrielle,  modélisation 3d, réetro-ingénierie, numérisation 3d, cad"/>
		<meta itemprop="url" content="https://3dpreciscan.com/contactez-nous"/>

<?php	
include('../includes/header.php');
?>


<section>

	<div class="contact clearfix">

		<div class="contactEnteteTitre">
			<h1>Nous joindre</h1>
		</div>	

		<div class="fullpagewidth clearfix">

			<div class="contactDescription">
				<!-- <h3>Notre processus rigoureux de scan 3D</h3> -->
				<p>Chez 3D Preciscan, notre valeur ajoutée consiste à <strong>répondre rapidement aux besoins du 
					client</strong> et à <strong>livrer des projets de qualité</strong>. Avec notre technologie, 
					nul besoin de vous déplacer chez le fournisseur: nous amenons la solution directement chez vous!</p>
				<p>Nous mettons notre expertise et notre professionnalisme à votre service pour livrer des mandats de
					qualité afin de développer des partenariats de confiance.</p>
				<!-- <p>Voici un aperçu des étapes de chacun de nos projets:</p> -->

			</div>
			

			<!-- <div class="aProposProcessusEtapeLigne clearfix">
				<div class="aProposProcessusEtape col1_2f">
					<div class="aProposProcessusEtapeTitre">
						<span>Étape 1</span>
						<h4>Contact client</h4>
					</div>

					<div class="aProposProcesusEtapeTxt">
						<p>Nous évaluons le besoin client ainsi que l’environnement dans lequel le scan devra s’effectuer. Cela nous 
							permet quantifier les détails de précision nécessaires et attendus à inclure dans le livrable. À cette étape, 
							l’envoi de plans de pièces CAD et/ou de photos aide à bien cerner le mandat et à faire une meilleure évaluation
							du temps de scan pour une soumission plus précise.</p>

					</div>

				</div>

				<div class="aProposProcessusEtape col1_2f">
					<div class="aProposProcessusEtapeTitre">
						<span>Étape 2</span>
						<h4>Devis</h4>
					</div>

					<div class="aProposProcesusEtapeTxt">
						<p>Dans cette étape du processus, nous répondons aux besoins du client préalablement exprimés lors de la première
							rencontre afin de fournir un devis sur mesure.</p>

					</div>

				</div>

			</div> -->

			<!-- <div class="aProposProcessusEtapeLigne clearfix">
				<div class="aProposProcessusEtape col1_2f">
					<div class="aProposProcessusEtapeTitre">
						<span>Étape 3</span>
						<h4>Scan</h4>
					</div>

					<div class="aProposProcesusEtapeTxt">
						<p>Lorsque le devis est accepté par le client, nous veillons à appliquer notre expertise: le scan!</p>

					</div>

				</div>

				<div class="aProposProcessusEtape col1_2f">
					<div class="aProposProcessusEtapeTitre">
						<span>Étape 4</span>
						<h4>Reconstruction</h4>
					</div>

					<div class="aProposProcesusEtapeTxt">
						<p>Cette étape est la technique qui permet d’obtenir une représentation en trois dimensions d’un objet à partir de la 
							pièce scannée. La reconstruction consiste à rebâtir le modèle (type shell) dans un format intelligible par un 
							logiciel de CAO (Conception assistée par ordinateur).</p>

					</div>

				</div>
			
			</div> -->

			<!-- <div class="aProposProcessusEtapeLigne clearfix">
				<div class="aProposProcessusEtape col1_2f">
					<div class="aProposProcessusEtapeTitre">
						<span>Étape 5</span>
						<h4>Analyse / Conclusion</h4>
					</div>

					<div class="aProposProcesusEtapeTxt">
						<p><strong>Dans le cas d’un service de rétro ingénierie:</strong>
						Nous nous assurons de la qualité du 3D reconstruit afin qu’il puisse être réutilisé par le client pour la 
						réalisation de son dessin 2D ou afin de continuer sa conception. <strong>Dans le cas d’une métrologie</strong>
						Nous veillons à l’analyse de la pièce scannée et la comparons au 3D théorique (si disponible) ou au plan 2D de la pièce.
						</p>
					</div>

				</div>

				<div class="aProposProcessusEtape col1_2f">
					<div class="aProposProcessusEtapeTitre">
						<span>Étape 6</span>
						<h4>Livraison des requis</h4>
					</div>

					<div class="aProposProcesusEtapeTxt">
						<p><strong>Dans le cas d’un service de rétro ingénierie:</strong>Nous fournissons un 
						modèle 3D utilisable par tout logiciel CAO. À la demande du client, nous pouvons 
						également fournir un dessin coté. <strong>Dans le cas d’une métrologie:</strong>
							Nous fournissions un rapport de contrôle de la pièce.
					</p>

					</div>

				</div>
		
			</div> -->

			<div class="contactDemandeSoumission">
				<a href="https://3dpreciscan.com/soumission" target="_self">
				<span>Obtenir une soumission </span>
				<span><i class="fa fa-arrow-circle-o-down" aria-hidden="true"></i></span>		
				</a>		
			</div>		

		</div>


		<div class="contactTitre">
			<!-- <h3>NOUS SOMMES À DRUMMONDVILLE, QUÉBEC</h3> -->
			<h3>Nos bureaux sont situés à Drummondville au Québec, mais on se déplace là où vous êtes!*</h3>
		</div>
		<div class="clearfix contactFContenu fullpagewidth">
			<!-- <div class="col1_2">
				<div class="contactFormulaire clearfix">

					<div class="contactFormulaireContenu  clearfix">

						<form action="/php/send-email.php" method="post" id="formulaireS">
							<input type="text" name="phone" class="ws_check" autocomplete="off">
							<input type="text" name="lang" value="fr" hidden>

							<label for="sNom" class="marginLabel"> Votre Nom (requis)</label>
							<input type="text" name="sNom" id="sNom" required>

							<label for="sCourriel" class="marginLabel"> Votre courriel (requis)</label>
							<input type="text" name="sCourriel" id="sCourriel" required>

							<label for="sTel" class="marginLabel"> Votre téléphone (requis)</label>
							<input type="text" name="sTel" id="sTel" required>

							<label for="sEntreprise" class="marginLabel"> Sujet</label>
							<input type="text" name="subject" id="sEntreprise" required>

							<label for="sMessage" class="marginLabel">Votre Message</label>
							<textarea name="sMessage" id="sMessage" cols="30" rows="10"></textarea>

							<div class="soumissionFormulaireBtn">
								<input type="submit">
							</div>
						</form>

					</div>

				</div>
			</div> -->
			
			<div class="col1_3f">
				<div class="contactInfos   clearfix">

					<div class="contactInfosTel ">
						
						<i class="fa fa-phone" aria-hidden="true"></i>
						<br>
						<span> Tél.: <a href="tel:+18193131152">1-819-313-1152</a></span>

					</div>

					<div class="contactInfosCoordos ">
						<i class="fa fa-map-marker" aria-hidden="true"></i>
						<br>
						<p><b>3D PRECISCAN</b> <br/>550 Rue Rocheleau Suite 300<br/>Drummondville, QC <br/>Canada, J2C 7V3</p>
						
						
					</div>

					<div class="contactInfosDeplacement">
						<i class="fa fa-car" aria-hidden="true"></i>
						<p><b>On se déplace là où vous êtes*</b></p>
					</div>

					<div class="contactInfosHoraire ">

						<i class="fa fa-clock-o" aria-hidden="true"></i>
						<br>
						<span><strong>Horaires réguliers:</strong></span> 

						<ul>
							<li><span><strong>LUNDI AU VENDREDI: 8h00 – 17h00</strong></span></li>
							<li><span><strong>SAMEDI ET DIMANCHE: FERMÉ</strong></span></li>
						</ul>

							<!-- <span>Horaires étendus*:</span> -->
							<!-- <br>
							<br> -->
							<span>Horaires étendus possibles sous certaines conditions.</span>
							<!-- <br>
							<br> -->
							<!-- <ul>
								<li><span>LUNDI AU VENDREDI: 17h00 – 22h00</span></li>
								<li><span>SAMEDI: 10h00 – 16h00</span></li>
							</ul> -->
								
						<!-- <span>*tarification majorée*</span> -->
								
					</div>	

				</div>
			</div>

			<div class="contactCarte col2_3r clearfix" id="map">

			</div>
		
		</div>
		
		<div class="clearfix contactFContenu fullpagewidth">

			<h3>Contactez-nous</h3>

			<div class="contactFormulaire clearfix">

				<div class="contactFormulaireContenu  clearfix">
					<?php if ($formError !== ''): ?>
						<div role="alert" style="margin-bottom:16px; padding:12px 14px; border:1px solid #b42318; background:#fff4f2; color:#7a271a; border-radius:4px;">
							<?php echo crm_html($formError); ?>
						</div>
					<?php endif; ?>

					<form action="/php/send-email.php" method="post" id="formulaireS">
						<input type="text" name="phone" class="ws_check" autocomplete="off" tabindex="-1" aria-hidden="true">
						<input type="text" name="lang" value="fr" hidden>
						<input type="hidden" name="submission_id" value="<?php echo crm_html($formSubmissionId); ?>">
						<input type="hidden" name="utm_source" id="utm_source">
						<input type="hidden" name="utm_medium" id="utm_medium">
						<input type="hidden" name="utm_campaign" id="utm_campaign">
						<input type="hidden" name="utm_content" id="utm_content">
						<input type="hidden" name="utm_term" id="utm_term">
						<input type="hidden" name="utm_id" id="utm_id">
						<input type="hidden" name="ad_id" id="ad_id">
						<input type="hidden" name="source_page_url" id="source_page_url">
						<input type="hidden" name="referrer_url" id="referrer_url">

						<label for="sNom" class="marginLabel"> Votre Nom (requis)</label>
						<input type="text" name="sNom" id="sNom" required minlength="2" maxlength="160" autocomplete="name">

						<label for="sCourriel" class="marginLabel"> Votre courriel (requis)</label>
						<input type="email" name="sCourriel" id="sCourriel" required maxlength="254" autocomplete="email">

						<label for="sTel" class="marginLabel"> Votre téléphone (requis)</label>
						<input type="tel" name="sTel" id="sTel" required maxlength="32" pattern="[0-9+(). \-]{7,32}" autocomplete="tel">

						<label for="sEntreprise" class="marginLabel"> Sujet</label>
						<input type="text" name="subject" id="sEntreprise" required maxlength="160">

						<label for="sMessage" class="marginLabel">Votre Message</label>
						<textarea name="sMessage" id="sMessage" cols="30" rows="10" maxlength="5000"></textarea>

						<label for="sms_consent" style="display:flex; align-items:flex-start; gap:8px; margin-top:16px; line-height:1.45; cursor:pointer;">
							<input type="checkbox" name="sms_consent" id="sms_consent" value="accepted" style="width:auto; margin-top:4px; flex:0 0 auto;">
							<span>J’accepte de recevoir des messages texte de 3D PreciScan au numéro fourni au sujet de ma demande. La fréquence varie. Des frais de messagerie peuvent s’appliquer. Mon consentement n’est pas une condition de service. Répondez STOP pour vous désabonner.</span>
						</label>

						<?php echo crm_turnstile_widget_html('contact'); ?>

						<div class="soumissionFormulaireBtn">
							<input type="submit">
						</div>
					</form>

				</div>

			</div>
		
		</div>
		
		<div class="fullpagewidth">	
			<span>*certaines conditions s’appliquent</span>
		</div>
		
		<!-- <div class="contactCarte fullpagewidth2 clearfix" id="map">

		</div> -->
		<div class="contactDemandeSoumission">
				<a href="https://3dpreciscan.com/soumission" target="_self">
				<span>Obtenir une soumission </span>
				<span><i class="fa fa-arrow-circle-o-down" aria-hidden="true"></i></span>		
				</a>		
			</div>	
			
	</div>

	<?php
		include "../includes/temoignages.php";

	?> 
		<div style="overflow:hidden;">
            <a href="https://3dpreciscan.com/realisations/arbre-oprant-sur-une-machine-enrouler-du-carton">.</a>
        </div>

</section>

<script>
(function () {
	const keys = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'utm_id', 'ad_id'];
	const params = new URLSearchParams(window.location.search);

	keys.forEach(function (key) {
		const value = params.get(key);
		if (value) sessionStorage.setItem(key, value);
	});

	if (!sessionStorage.getItem('source_page_url')) {
		sessionStorage.setItem('source_page_url', window.location.href);
	}
	if (document.referrer && !sessionStorage.getItem('referrer_url')) {
		sessionStorage.setItem('referrer_url', document.referrer);
	}

		document.querySelectorAll('form').forEach(function (form) {
			keys.concat(['source_page_url', 'referrer_url']).forEach(function (key) {
				const input = form.querySelector('[name="' + key + '"]');
				if (input) input.value = sessionStorage.getItem(key) || '';
			});
		});

	document.querySelectorAll('a[href]').forEach(function (link) {
		try {
			const url = new URL(link.href, window.location.origin);
			if (url.hostname !== window.location.hostname) return;
			if (!url.pathname.includes('soumission') && !url.pathname.includes('contactez-nous')) return;

			keys.forEach(function (key) {
				const value = sessionStorage.getItem(key);
				if (value && !url.searchParams.get(key)) url.searchParams.set(key, value);
			});
			link.href = url.toString();
		} catch (error) {
			return;
		}
	});
})();
</script>



<?php
include "../includes/footer.php";
?>
