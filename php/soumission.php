	<?php
		include ("../includes/tophead.php");
		$page = 'soumission';
		//sitemap
	?>
	
	<link rel="canonical" href="https://3dpreciscan.com/soumission">
	
	<title>
		Soumission - Ingénierie, Scan 3D et Impression 3D - 3D PreciScan
	</title>  
					
	<meta name="description" content="Que ce soit pour une soumission en ingénierie, scan 3D, impression 3D ou un service clé en main, vous pouvez compter sur nos professionnels en ingénierie certifier. ">
					
	<meta name="keywords" content="soumission, devis, prix, scan 3d, numérisation 3d, fea, reconstruction 3d, conception 3d, laser tracker, impression 3d, ingénierie, inspection 3d, métrologie industrielle,  modélisation 3d, réetro-ingénierie, numérisation 3d, cad">

	<meta property="og:title" content="Soumission en ligne" />
	<meta property="og:description" content="Découvrez une panoplie de produits créés par des artisans du Québec." /> 
	<meta property="og:site_name"  content="3D PresiScan Inc." />
	<meta property="og:locale"  content="fr_CA" />
	<meta property="og:type" content="website" />
	<meta property="og:url" content="https://3dpreciscan.com/soumission" />
	<!--meta property="og:image" content="https://creationsdici.ca/images/creations-dici/banner_rs1.jpg" /-->
	
	<?php
		include "../includes/head.php";
		//SEO
		include "../includes/shemas/soumission-fr.php";
		include('../includes/google.php');
	?>

	<body itemscope itemtype="http://schema.org/WebPage">
		<noscript>
			<iframe src="https://www.googletagmanager.com/ns.html?id=GTM-NBH3JPT" height="0" width="0" style="display:none;visibility:hidden"></iframe>
		</noscript>
		<meta itemprop="name" content="Soumission en ligne"/>
		<!--meta itemprop="description" content=""/>
		<meta itemprop="images" content=""/-->
		<meta itemprop="keywords" content="soumission, devis, prix, scan 3d, numérisation 3d, fea, reconstruction 3d, conception 3d, laser tracker, impression 3d, ingénierie, inspection 3d, métrologie industrielle,  modélisation 3d, réetro-ingénierie, numérisation 3d, cad"/>
		<meta itemprop="url" content="https://3dpreciscan.com/soumission"/>

		<?php	
			include('../includes/header.php');
		?>


<section class="quote-hero" aria-labelledby="quote-hero-title">
    <div class="quote-hero__container">
        <span class="quote-hero__subtitle">Réponse rapide et précise</span>
        <h1 id="quote-hero-title" class="quote-hero__title">Soumission 100% Gratuite</h1>
        <div class="quote-hero__accent"></div>
    </div>
</section>

<section class="quote-form-section" aria-label="Formulaire de demande de soumission">
    <div class="quote-form-container">
        
        <div class="quote-form-intro">
            <h2>Obtenez votre soumission en remplissant le formulaire suivant :</h2>
            <p>Veuillez fournir les détails de votre projet ci-dessous, et nos experts analyseront vos besoins.</p>
        </div>

        <div id="toast-container" style="position: fixed; top: 20px; right: 20px; z-index: 999999;"></div>

        <form action="/php/send-email-soumission.php" method="post" id="formulaireS" enctype="multipart/form-data" class="modern-quote-form" novalidate>
                    
            <div class="form-grid">
                
                <div class="form-card">
                    <h3 class="form-card__title">1. Informations de contact</h3>
                    
                    <div class="form-group">
                        <label class="form-group__label">Secteur d'activité *</label>
                        <div class="radio-group" id="group-type-client">
                            <label class="radio-option">
                                <input type="radio" class="type_client" name="type_client" value="Company">
                                <span class="radio-custom"></span>
                                Entreprise
                            </label>
                            <label class="radio-option">
                                <input type="radio" class="type_client" name="type_client" value="Individual">
                                <span class="radio-custom"></span>
                                Particulier
                            </label>
                        </div>
                    </div>

                    <div class="form-group">
                        <label for="sNom" class="form-group__label">Votre nom (et nom de l'entreprise si applicable) *</label>
                        <input type="text" name="sNom" id="sNom" required minlength="3" placeholder="John Doe / Acme Corp" class="form-control">
                        <input type="text" name="phone" class="ws_check" autocomplete="off" style="display:none !important;">
                    </div>

                    <div class="form-group">
                        <label for="sTel" class="form-group__label">Votre numéro de téléphone *</label>
                        <input type="tel" name="sTel" id="sTel" required pattern="^[0-9-+s() ]{7,20}$" placeholder="819-313-1152" class="form-control">
                    </div>

                    <div class="form-group">
                        <label for="sCourriel" class="form-group__label">Votre adresse courriel *</label>
                        <input type="email" name="sCourriel" id="sCourriel" required placeholder="exemple@domaine.com" class="form-control">
                    </div>
                </div>

                <div class="form-card">
                    <h3 class="form-card__title">2. Détails de l'objet et spécifications</h3>
                    
                    <div class="form-group">
                        <label class="form-group__label">Dimensions de l'objet *</label>
                        <div class="dimensions-row">
                            <input type="text" name="hauteur" id="dim-h" placeholder="Hauteur" required class="form-control">
                            <input type="text" name="largeur" id="dim-w" placeholder="Largeur" required class="form-control">
                            <input type="text" name="longueur" id="dim-l" placeholder="Longueur" required class="form-control">
                            <input type="text" name="autres" id="dim-o" placeholder="Autres" class="form-control">
                        </div>
                    </div>

                    <div class="form-group">
                        <label class="form-group__label">Joindre des fichiers (Images, STL...)</label>
                        <div class="file-upload-wrapper js">
                            <input type="file" name="images[]" id="images" class="inputfile inputfile-1" multiple="multiple">
                            <label for="images" class="file-upload-trigger">
                                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="17" viewBox="0 0 20 17" fill="currentColor">
                                    <path d="M10 0l-5.2 4.9h3.3v5.1h3.8v-5.1h3.3l-5.2-4.9zm9.3 11.5l-3.2-2.1h-2l3.4 2.6h-3.5c-.1 0-.2.1-.2.1l-.8 2.3h-6l-.8-2.2c-.1-.1-.1-.2-.2-.2h-3.6l3.4-2.6h-2l-3.2 2.1c-.4.3-.7 1-.6 1.5l.6 3.1c.1.5.7.9 1.2.9h16.3c.6 0 1.1-.4 1.3-.9l.6-3.1c.1-.5-.2-1.2-.7-1.5z"/>
                                </svg> 
                                <span>Joindre des images + fichiers STL&hellip;</span>
                            </label>
                            <ul class="images-preview-list images"></ul>
                        </div>
                    </div>

                    <div class="form-group">
                        <label for="sDelai" class="form-group__label">Délai de réalisation souhaité?</label>
                        <input type="text" name="sDelai" id="sDelai" placeholder="ex. : 2 semaines" class="form-control">
                    </div>

                    <div class="form-group">
                        <label class="form-group__label">La demande est-elle urgente? *</label>
                        <div class="radio-group" id="group-urgent">
                            <label class="radio-option">
                                <input type="radio" name="urgent" value="yes">
                                <span class="radio-custom"></span>
                                Oui
                            </label>
                            <label class="radio-option">
                                <input type="radio" name="urgent" value="no">
                                <span class="radio-custom"></span>
                                Non
                            </label>
                        </div>
                    </div>
                </div>

            </div>

            <div class="form-card form-card--fullwidth">
                <h3 class="form-card__title">3. Sélectionner les services requis *</h3>
                
                <div class="checkbox-grid" id="services-checkbox-group">
                    <label class="checkbox-option">
                        <input type="checkbox" name="Scan3d" id="Scan3d" value="yesclbase">
                        <span class="checkbox-custom"></span>
                        Scan 3D 
                    </label>
                    <label class="checkbox-option">
                        <input type="checkbox" name="Rec3d" id="Rec3d" value="yesclbase">
                        <span class="checkbox-custom"></span>
                        Reconstruction 3D
                    </label>
                    <label class="checkbox-option">
                        <input type="checkbox" name="R_ing3d2d" id="R_ing3d2d" value="yesclbase">
                        <span class="checkbox-custom"></span>
                        Rétro-ingénierie (3D & 2D)
                    </label>
                    <label class="checkbox-option">
                        <input type="checkbox" name="Inspection" id="Inspection" value="yesclbase">
                        <span class="checkbox-custom"></span>
                        Inspection 3D
                    </label>
                    <label class="checkbox-option">
                        <input type="checkbox" name="Metrologie" id="Metrologie" value="yesclbase">
                        <span class="checkbox-custom"></span>
                        Métrologie industrielle
                    </label>
                    <label class="checkbox-option">
                        <input type="checkbox" name="Concep2d3d" id="Concep2d3d" value="yesclbase">
                        <span class="checkbox-custom"></span>
                        Conception (3D & 2D)
                    </label>
                    <label class="checkbox-option">
                        <input type="checkbox" name="Modelisation3D" id="Modelisation3D" value="yesclbase">
                        <span class="checkbox-custom"></span>
                        Modélisation
                    </label>
                    <label class="checkbox-option">
                        <input type="checkbox" name="Ingenerie" id="Ingenerie" value="yesclbase">
                        <span class="checkbox-custom"></span>
                        Ingénierie
                    </label>
                    <label class="checkbox-option">
                        <input type="checkbox" name="Imp3d" id="Imp3d" value="yesclbase">
                        <span class="checkbox-custom"></span>
                        Impression 3D
                    </label>
                    <label class="checkbox-option">
                        <input type="checkbox" name="Simul3D" id="Simul3D" value="yesclbase">
                        <span class="checkbox-custom"></span>
                        Simulation 3D
                    </label>
                    <label class="checkbox-option">
                        <input type="checkbox" name="Aucun" id="Aucun" value="yesclbase">
                        <span class="checkbox-custom"></span>
                        Je ne sais pas
                    </label>
                </div>

                <div class="form-group" style="margin-top: 30px;">
                    <label for="info" class="form-group__label">Informations additionnelles</label>
                    <textarea name="info" id="info" rows="6" placeholder="Décrivez votre projet, les détails du matériau, les tolérances requises, etc..." class="form-control form-control--textarea"></textarea>
                </div>

                <div class="form-submit-wrapper">
                    <input type="submit" value="Envoyer la demande" class="form-submit-btn" id="submit-btn">
                </div>
            </div>

            <input type="text" name="lang" value="fr" hidden> 
            <input type="hidden" name="utm_source" id="utm_source">
            <input type="hidden" name="utm_medium" id="utm_medium">
            <input type="hidden" name="utm_campaign" id="utm_campaign">
            <input type="hidden" name="utm_content" id="utm_content">
            <input type="hidden" name="utm_term" id="utm_term">
            <input type="hidden" name="utm_id" id="utm_id">
            <input type="hidden" name="ad_id" id="ad_id">
            <input type="hidden" name="source_page_url" id="source_page_url">
            <input type="hidden" name="referrer_url" id="referrer_url">
        </form>

    </div>
</section>

<section class="quote-team-closure" aria-label="Salutations de l'équipe">
    <div class="quote-team-closure__container">
        <div class="quote-team-closure__content">
            <h3 class="quote-team-closure__text">
                Au plaisir de vous rencontrer bientôt!
                <span>- L'équipe de 3D PreciScan</span>
            </h3>
        </div>
        <div class="quote-team-closure__image-wrapper">
            <img src="../images/redesign/3D_Preciscan_mai23_0616.webp" alt="L'équipe d'experts de 3D PreciScan prête à vous aider" class="quote-team-closure__img">
        </div>
    </div>
</section>

<script>
(function () {
    const keys = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'utm_id', 'ad_id'];
    const params = new URLSearchParams(window.location.search);

    keys.forEach((key) => {
        const value = params.get(key);
        if (value) sessionStorage.setItem(key, value);
    });

    if (!sessionStorage.getItem('source_page_url')) {
        sessionStorage.setItem('source_page_url', window.location.href);
    }
    if (document.referrer && !sessionStorage.getItem('referrer_url')) {
        sessionStorage.setItem('referrer_url', document.referrer);
    }

    document.querySelectorAll('form').forEach((form) => {
        keys.concat(['source_page_url', 'referrer_url']).forEach((key) => {
            const input = form.querySelector(`[name="${key}"]`);
            if (input) input.value = sessionStorage.getItem(key) || '';
        });
    });
})();

document.getElementById('formulaireS').addEventListener('submit', function(e) {
    let errors = [];
    
    const typeClientChecked = document.querySelector('input[name="type_client"]:checked');
    const groupTypeClient = document.getElementById('group-type-client');
    if (!typeClientChecked) {
        errors.push({ msg: 'Veuillez sélectionner un secteur d\'activité.', element: groupTypeClient });
        applyContainerErrorStyle(groupTypeClient);
    } else {
        resetContainerStyle(groupTypeClient);
    }

    const requiredInputs = document.querySelectorAll('#formulaireS input[required]');
    requiredInputs.forEach(input => {
        if (!input.value.trim() || (input.pattern && !new RegExp(input.pattern).test(input.value))) {
            errors.push({ msg: `Veuillez remplir correctement le champ : ${input.placeholder || 'Champ obligatoire'}`, element: input });
            input.style.borderColor = '#cc0000';
            input.style.backgroundColor = '#fff8f8';
        } else {
            input.style.borderColor = '#e2e8f0';
            input.style.backgroundColor = '#ffffff';
        }
    });

    const urgentChecked = document.querySelector('input[name="urgent"]:checked');
    const groupUrgent = document.getElementById('group-urgent');
    if (!urgentChecked) {
        errors.push({ msg: 'Veuillez spécifier si la demande est urgente.', element: groupUrgent });
        applyContainerErrorStyle(groupUrgent);
    } else {
        resetContainerStyle(groupUrgent);
    }

    const checkboxes = document.querySelectorAll('#services-checkbox-group input[type="checkbox"]');
    let isServicesChecked = false;
    checkboxes.forEach(cb => { if (cb.checked) isServicesChecked = true; });
    
    const groupServices = document.getElementById('services-checkbox-group');
    if (!isServicesChecked) {
        errors.push({ msg: 'Veuillez sélectionner au moins un service dans la section 3.', element: groupServices });
        applyContainerErrorStyle(groupServices);
    } else {
        resetContainerStyle(groupServices);
    }

    if (errors.length > 0) {
        e.preventDefault(); 
        showToastPopup(errors[0].msg);
        
        errors[0].element.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
});

function applyContainerErrorStyle(el) {
    el.style.outline = '2px solid #cc0000';
    el.style.padding = '12px';
    el.style.borderRadius = '8px';
    el.style.backgroundColor = '#fff8f8';
}

function resetContainerStyle(el) {
    el.style.outline = 'none';
    el.style.padding = '0';
    el.style.backgroundColor = 'transparent';
}

function showToastPopup(message) {
    const container = document.getElementById('toast-container');
    container.innerHTML = ''; 
    
    const toast = document.createElement('div');
    toast.className = 'custom-toast-error';
    toast.innerHTML = `
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" style="color:#cc0000; flex-shrink:0;">
            <path d="M12 22C17.5228 22 22 17.5228 22 12C22 6.47715 17.5228 2 12 2C6.47715 2 2 6.47715 2 12C2 17.5228 6.47715 22 12 22Z" stroke="currentColor" stroke-width="2"/>
            <path d="M12 8V12" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
            <circle cx="12" cy="16" r="1.25" fill="currentColor"/>
        </svg>
        <p class="custom-toast-error__text">${message}</p>
    `;
    
    container.appendChild(toast);
    
    setTimeout(() => {
        toast.style.animation = 'slideOutRight 0.3s ease forwards';
        setTimeout(() => toast.remove(), 300);
    }, 4500);
}

document.querySelectorAll('input[name="type_client"]').forEach(radio => {
    radio.addEventListener('change', () => resetContainerStyle(document.getElementById('group-type-client')));
});
document.querySelectorAll('input[name="urgent"]').forEach(radio => {
    radio.addEventListener('change', () => resetContainerStyle(document.getElementById('group-urgent')));
});
document.querySelectorAll('#services-checkbox-group input[type="checkbox"]').forEach(cb => {
    cb.addEventListener('change', () => resetContainerStyle(document.getElementById('services-checkbox-group')));
});
</script>

	<?php
			include "../includes/temoignages.php";
			?> 
		




<?php
include "../includes/footer.php";
?>