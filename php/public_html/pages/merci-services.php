<?php
	include ("../includes/tophead.php");
	$page = 'services';
?>

<meta name="robots" content="noindex,follow">
<title>Demande reçue | 3D PreciScan</title>

<?php include "../includes/head.php"; ?>
<style>
	.confirmation-hero { padding-top: clamp(56px, 8vw, 88px); padding-bottom: clamp(56px, 8vw, 88px); }
	.confirmation-hero .quote-hero__container { max-width: 900px; }
	.confirmation-hero .quote-hero__title { margin-bottom: 0; }
	.confirmation-page { padding: clamp(48px, 7vw, 80px) 20px clamp(64px, 9vw, 104px); background: #f9f9f9; }
	.confirmation-page__container { box-sizing: border-box; width: 100%; max-width: 900px; margin: 0 auto; }
	.confirmation-card { position: relative; box-sizing: border-box; width: 100%; margin: 0; padding: clamp(32px, 6vw, 58px); overflow: hidden; border-radius: 12px; background: #fff; box-shadow: 0 18px 50px rgba(36, 45, 56, 0.1); text-align: center; }
	.confirmation-card::before { position: absolute; top: 0; left: 0; width: 100%; height: 5px; background: var(--color-primary, #f0542d); content: ""; }
	.confirmation-card__icon { display: inline-flex; align-items: center; justify-content: center; width: 68px; height: 68px; margin-bottom: 24px; border-radius: 50%; background: rgba(240, 84, 45, 0.1); color: var(--color-primary, #f0542d); }
	.confirmation-card__icon svg { width: 34px; height: 34px; }
	.confirmation-card__title { max-width: 680px; margin: 0 auto 18px; color: #242d38; font-family: Raleway, Arial, sans-serif; font-size: clamp(27px, 4vw, 38px); font-weight: 700; line-height: 1.18; }
	.confirmation-card__message { max-width: 660px; margin: 0 auto; color: #596575; font-size: clamp(16px, 2vw, 18px); line-height: 1.7; }
	.confirmation-card__urgent { max-width: 660px; margin: 30px auto 0; padding: 18px 22px; border: 1px solid rgba(240, 84, 45, 0.2); border-radius: 10px; background: #fff7f4; color: #3d4855; font-size: 16px; line-height: 1.55; }
	.confirmation-card__urgent a { color: #c63d1d; font-weight: 700; text-decoration: none; }
	.confirmation-card__urgent a:hover { text-decoration: underline; }
	.confirmation-card__actions { display: flex; flex-wrap: wrap; justify-content: center; gap: 14px; margin-top: 34px; }
	.confirmation-card__button { box-sizing: border-box; display: inline-flex; align-items: center; justify-content: center; min-width: 210px; min-height: 50px; padding: 13px 24px; border: 2px solid transparent; border-radius: 6px; font-family: Raleway, Arial, sans-serif; font-size: 15px; font-weight: 700; line-height: 1.2; text-decoration: none; transition: background-color 180ms ease, border-color 180ms ease, color 180ms ease, transform 180ms ease; }
	.confirmation-card__button--primary { background: var(--color-primary, #f0542d); color: #fff; }
	.confirmation-card__button--primary:hover { background: var(--color-primary-hover, #d94723); color: #fff; transform: translateY(-2px); }
	.confirmation-card__button--secondary { border-color: #d8dde3; background: #fff; color: #364352; }
	.confirmation-card__button--secondary:hover { border-color: var(--color-primary, #f0542d); color: var(--color-primary, #f0542d); transform: translateY(-2px); }
	.confirmation-card__button:focus-visible { outline: 3px solid rgba(240, 84, 45, 0.3); outline-offset: 3px; }
	@media (max-width: 640px) { .confirmation-card { padding-right: 24px; padding-left: 24px; } .confirmation-card__actions { flex-direction: column; } .confirmation-card__button { width: 100%; min-width: 0; } }
	@media (prefers-reduced-motion: reduce) { .confirmation-card__button { transition: none; } }
</style>
<?php include('../includes/google.php'); ?>

<body>
	<noscript>
		<iframe src="https://www.googletagmanager.com/ns.html?id=GTM-NBH3JPT" height="0" width="0" style="display:none;visibility:hidden"></iframe>
	</noscript>

	<?php include('../includes/header.php'); ?>

	<main>
		<section class="quote-hero confirmation-hero" aria-labelledby="merci-title">
			<div class="quote-hero__container">
				<span class="quote-hero__subtitle">Demande reçue</span>
				<h1 id="merci-title" class="quote-hero__title">Merci&nbsp;!</h1>
				<div class="quote-hero__accent" aria-hidden="true"></div>
			</div>
		</section>

		<section class="quote-form-section confirmation-page" aria-labelledby="merci-confirmation-title">
			<div class="quote-form-container confirmation-page__container">
				<article class="form-card confirmation-card">
					<div class="confirmation-card__icon" aria-hidden="true">
						<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M20 6 9 17l-5-5" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/></svg>
					</div>
					<h2 id="merci-confirmation-title" class="confirmation-card__title">Votre demande a bien été reçue.</h2>
					<p class="confirmation-card__message">Merci de votre intérêt pour nos services. Un membre de notre équipe communiquera avec vous dans un délai de 12 à 24 heures ouvrables.</p>
					<p class="confirmation-card__urgent">Votre projet est urgent&nbsp;? Appelez-nous au <a href="tel:+18193131152">819&nbsp;313-1152</a>.</p>
					<div class="confirmation-card__actions">
						<a href="/" class="btn btn--primary confirmation-card__button confirmation-card__button--primary">Retour à l’accueil</a>
						<a href="/services" class="btn btn--secondary confirmation-card__button confirmation-card__button--secondary">Découvrir nos services</a>
					</div>
				</article>
			</div>
		</section>
	</main>

	<?php include "../includes/footer.php"; ?>
