<?php

function contenu_url($url)
{
	global $prefix_session;
	$where = array('Ancien_url' => $url);
	$where_arg = array('=');
	$reponse = req_mysql('S', 'redirections', false, $where, $where_arg, false, false);
	if($donnees = lire_data_mysql($reponse))
	{

		$id = $donnees['ID_contenu'];
	}
	else
	{

		$where = array('URL' => $url);
		$where_arg = array('=');
		$reponse = req_mysql('S', 'contenu', false, $where, $where_arg, false, false);
		if($donnees = lire_data_mysql($reponse))
		{
			$id = $donnees['ID'];
		}
	}

	if(!isset($id))
	{
		header("Location:/erreur");
	}
	else
	{

		$where = array('ID' => $id, 'Langue' => $_SESSION[$prefix_session . '_langue']);
		$where_arg = array('=', 'AND;=');
		$reponse = req_mysql('S', 'contenu', false, $where, $where_arg, false, false);
		$donnees = lire_data_mysql($reponse);

		$data = array();
		$reponse_bd = champs_table_bd('contenu');
		while($donnees_bd = lire_data_mysql($reponse_bd))
		{
			$data[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);
		}

		return $data;
	}

}

function contenu_cs($id, $type)
{

	global $prefix_session;
	$where = array('Custom_type' => $type, 'Langue' => $_SESSION[$prefix_session . '_langue']);
	$where_arg = array('=', 'AND;=');
	$reponse = req_mysql('S', 'contenu_types', false, $where, $where_arg, false, false);
	if($donnees = lire_data_mysql($reponse))
	{
		//echo $donnees['ID_types'];
		$cs = array();
		$where = array('ID_types' => $donnees['ID_types']);
		$where_arg = array('=');
		$reponse_cs = req_mysql('S', 'contenu_types_cs', false, $where, $where_arg, 'ORDER BY Ordre', false);
		while($donnees_cs = lire_data_mysql($reponse_cs))
		{
			if($donnees_cs['ID_C'] == '0' OR $donnees_cs['ID_C'] == $id)
				$cs[] = array('ID' => 'CS_' . $donnees_cs['ID'], 'ID_BD' => $donnees_cs['ID'], 'Champs' => ($donnees_cs['Champs']), 'Type_champs' => $donnees_cs['Type_champs']);
		}
		/*echo '<pre>';
		print_r($cs);
		echo '</pre>';//*/
		$contenu = array();
		foreach($cs AS $k => $v)
		{
			$where = array('ID_CS' => $v['ID_BD'], 'ID_C' => $id, 'Lan' => $_SESSION[$prefix_session . '_langue']);
			$where_arg = array('=', 'AND;=', 'AND;=');
			$reponse_cs = req_mysql('S', 'contenu_cs', false, $where, $where_arg, false, false);
			if($donnees_cs = lire_data_mysql($reponse_cs))
				$contenu[$v['ID']] = (str_replace('&quot;', '"', $donnees_cs['Contenu']));
			else
				$contenu[$v['ID']] = '';
		}

		return $contenu;

	}
	else
		return array();
}

function contenu_bloc($id)
{
	global $prefix_session;
	$dossier_maitre = dossier_maitre();
	$flag = true;
	$where = array('ID_C' => $id, 'Langue' => $_SESSION[$prefix_session . '_langue']);
	$where_arg = array('=', 'AND;=');
	$reponse = req_mysql('S', 'contenu_bloc', false, $where, $where_arg, 'Order by Position', false);
	while($donnees = lire_data_mysql($reponse))
	{
		$data = array();
		$reponse_bd = champs_table_bd('contenu_bloc');
		while($donnees_bd = lire_data_mysql($reponse_bd))
		{
			$data[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);
		}

		switch($donnees['Disposition'])
		{
			case 'txt_full_width' :

				?>
				<div class="txt_full_width bloc<?php echo $donnees['ID'] ?>">
					<?php echo $data['Contenu1']?>
				</div>
				<?php
			break;


			case 'img_full_width' :
				?>
				<div class="img_full_width bloc<?php echo $donnees['ID'] ?>">

					<img src="<?php echo $dossier_maitre?>/php/filemanager/source/<?php echo $data['Contenu1']?>"/>

				</div>
				<?php
			break;

			case 'vid_full_width' :
				?>
				<div class="vid_full_width bloc<?php echo $donnees['ID'] ?>">

					<div class="videoWrapper">
						<?php echo $data['Contenu1']?>
					</div>

				</div>
				<?php
			break;

			case 'slider_full_width' :
				?>
				<div class="slider_full_width bloc<?php echo $donnees['ID'] ?>">

					<div class="bloc_slider">
						<?php

						$images = explode(',', str_replace('[', '', str_replace(']', '', str_replace('"', '', trim($data['Contenu1'])))));
						foreach($images AS $i =>$w)
						{
							?>
							<div>
								<img src="<?php echo $dossier_maitre?>/php/filemanager/source/<?php echo $w?>">
							</div>
							<?php
						}
						?>

					</div>

				</div>
				<?php
			break;


			case '2coltxt' :
				?>
				<div class="2coltxt bloc<?php echo $donnees['ID'] ?> clearfix">

					<div class="col1_2">
						<?php echo $data['Contenu1']?>
					</div>
					<div class="col1_2r">
						<?php echo $data['Contenu2']?>
					</div>

				</div>
				<?php
			break;

			case '2coltxtp' :
				$largeur_col_1 = $data['Contenu3'];
				$largeur_col_2 = 100 - $largeur_col_1 - 5; //5% est la marge entre les 2

				?>
				<div class="2coltxtp bloc<?php echo $donnees['ID'] ?> clearfix">

					<div class="col_1" 	style="width:<?php echo $largeur_col_1?>%">
						<?php echo $data['Contenu1']?>
					</div>
					<div class="col_2" style="width:<?php echo $largeur_col_2?>%">
						<?php echo $data['Contenu2']?>
					</div>

				</div>
				<?php
			break;

			case '3coltxt' :
				?>
				<div class="3coltxt bloc<?php echo $donnees['ID'] ?> clearfix">

					<div class="col1_3">
						<?php echo $data['Contenu1']?>
					</div>
					<div class="col1_3">
						<?php echo $data['Contenu2']?>
					</div>
					<div class="col1_3r">
						<?php echo $data['Contenu3']?>
					</div>

				</div>
				<?php
			break;

			case '3coltxtp' :
				$largeur_col_1 = $data['Contenu4'];
				$largeur_col_3 = $data['Contenu5'];
				$largeur_col_2 = 100 - $largeur_col_1 - $largeur_col_3 - 6; //3% est la marge

				?>
				<div class="3coltxtp bloc<?php echo $donnees['ID'] ?> clearfix">

					<div class="col_1" 	style="width:<?php echo $largeur_col_1?>%">
						<?php echo $data['Contenu1']?>
					</div>
					<div class="col_2" style="width:<?php echo $largeur_col_2?>%">
						<?php echo $data['Contenu2']?>
					</div>
					<div class="col_3" style="width:<?php echo $largeur_col_3?>%">
						<?php echo $data['Contenu3']?>
					</div>

				</div>
				<?php
			break;

			case '4coltxt' :
				?>
				<div class="4coltxt bloc<?php echo $donnees['ID'] ?> clearfix">

					<div class="col1_4">
						<?php echo $data['Contenu1']?>
					</div>
					<div class="col1_4">
						<?php echo $data['Contenu2']?>
					</div>
					<div class="col1_4">
						<?php echo $data['Contenu3']?>
					</div>
					<div class="col1_4r">
						<?php echo $data['Contenu4']?>
					</div>

				</div>
				<?php
			break;

			case 'img_gauche' :
				?>
				<div class="img_gauche bloc<?php echo $donnees['ID'] ?> clearfix">

					<div class="left">
						<img src="<?php echo $dossier_maitre?>/php/filemanager/source/<?php echo $data['Contenu2']?>"/>
					</div>
					<div class="right">
						<div class="text_right">
							<?php echo $data['Contenu1']?>
						</div>
					</div>


				</div>
				<?php
			break;

			case 'img_droite' :
				?>
				<div class="img_droite bloc<?php echo $donnees['ID'] ?> clearfix">

					<div class="left">
						<div class="text_left">
							<?php echo $data['Contenu1']?>
						</div>
					</div>

					<div class="right">
						<img src="<?php echo $dossier_maitre?>/php/filemanager/source/<?php echo $data['Contenu2']?>"/>
					</div>


				</div>
				<?php
			break;

			case 'vid_gauche' :
				?>
				<div class="vid_gauche bloc<?php echo $donnees['ID'] ?> clearfix">

					<div class="left">
						<div class="videoWrapper">
							<?php echo $data['Contenu2']?>
						</div>
					</div>
					<div class="right">
						<div class="text_right">
							<?php echo $data['Contenu1']?>
						</div>
					</div>

				</div>
				<?php
			break;

			case 'vid_droite' :
				?>
				<div class="vid_droite bloc<?php echo $donnees['ID'] ?> clearfix">

					<div class="right">
						<div class="videoWrapper">
							<?php echo $data['Contenu2']?>
						</div>
					</div>
					<div class="left">
						<div class="text_left">
							<?php echo $data['Contenu1']?>
						</div>
					</div>

				</div>
				<?php
			break;

			case 'slider_gauche' :
				?>
				<div class="slider_gauche bloc<?php echo $donnees['ID'] ?> clearfix">

					<div class="left">
						<div class="bloc_slider">
							<?php

							$images = explode(',', str_replace('[', '', str_replace(']', '', str_replace('"', '', trim($data['Contenu2'])))));
							foreach($images AS $i =>$w)
							{
								?>
								<div>
									<img src="<?php echo $dossier_maitre?>/php/filemanager/source/<?php echo $w?>">
								</div>
								<?php
							}
							?>

						</div>
					</div>
					<div class="right">
						<div class="text_right">
							<?php echo $data['Contenu1']?>
						</div>
					</div>

				</div>
				<?php
			break;

			case 'slider_droite' :
				?>
				<div class="slider_droite bloc<?php echo $donnees['ID'] ?> clearfix">

					<div class="right">
						<div class="bloc_slider">
							<?php

							$images = explode(',', str_replace('[', '', str_replace(']', '', str_replace('"', '', trim($data['Contenu2'])))));
							foreach($images AS $i =>$w)
							{
								?>
								<div>
									<img src="<?php echo $dossier_maitre?>/php/filemanager/source/<?php echo $w?>">
								</div>
								<?php
							}
							?>

						</div>
					</div>
					<div class="left">
						<div class="text_left">
							<?php echo $data['Contenu1']?>
						</div>
					</div>

				</div>
				<?php
			break;


		}
	}
}

function contenu_cat_produits($nb = 0)
{
	global $prefix_session;

	$data = array();

	if($nb == 0)
		$limit = false;
	else
		$limit = "LIMIT 0, " . $nb;

	$where = array('Type' => 'produits_categorie', 'Langue' => $_SESSION[$prefix_session . '_langue'], 'Position' => '5');
	$where_arg = array('=', 'AND;=', 'AND;<=');
	$reponse = req_mysql('S', 'contenu', false, $where, $where_arg, 'Order by Position', $limit);
	while($donnees = lire_data_mysql($reponse))
	{
		$contenu = array();
		$reponse_bd = champs_table_bd('contenu');
		while($donnees_bd = lire_data_mysql($reponse_bd))
		{
			$contenu[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);
		}

		$data[] = $contenu;

	}

	return $data;
}

function contenu_cat_produits_pos($pos)
{
	global $prefix_session;


	$data = array();


	if($pos == 0)
	{
		$where = array('Type' => 'produits_categorie', 'Langue' => $_SESSION[$prefix_session . '_langue'], 'Position' => '5');
		$where_arg = array('=', 'AND;=', 'AND;<=');
		$reponse = req_mysql('S', 'contenu', 'Max(Position) AS max_pos', $where, $where_arg);
		$donnees = lire_data_mysql($reponse);
		$pos = $donnees['max_pos'];
	}



	$where = array('Type' => 'produits_categorie', 'Langue' => $_SESSION[$prefix_session . '_langue'], 'Position' => $pos);
	$where_arg = array('=', 'AND;=', 'AND;=');
	$reponse = req_mysql('S', 'contenu', false, $where, $where_arg);
	if($donnees = lire_data_mysql($reponse))
	{
		$contenu = array();
		$reponse_bd = champs_table_bd('contenu');
		while($donnees_bd = lire_data_mysql($reponse_bd))
		{
			$contenu[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);
		}
	}
	else
	{
		$pos = 1;
		$where = array('Type' => 'produits_categorie', 'Langue' => $_SESSION[$prefix_session . '_langue'], 'Position' => $pos);
		$where_arg = array('=', 'AND;=', 'AND;=');
		$reponse = req_mysql('S', 'contenu', false, $where, $where_arg);
		$donnees = lire_data_mysql($reponse);
		$contenu = array();
		$reponse_bd = champs_table_bd('contenu');
		while($donnees_bd = lire_data_mysql($reponse_bd))
		{
			$contenu[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);
		}

	}

	$image_size = getimagesize($_SERVER['DOCUMENT_ROOT'] . "/images/surplus-armee-dunham/" . $contenu['Image']);

	if($image_size[0] > $image_size[1])
	{
		$ratio = number_format($image_size[0] / $image_size[1],1,'.','');
		if($ratio >= 1.75)
			$contenu['Type_img'] = 'paysage';
		else
			$contenu['Type_img'] = '';

	}
	else
		$contenu['Type_img'] = '';

	$data = $contenu;



	return $data;
}

function contenu_scat_produits($id_cat)
{
	global $prefix_session;

	$data = array();


	$where = array('ID_C' => $id_cat, 'Langue' => $_SESSION[$prefix_session . '_langue']);
	$where_arg = array('=', 'AND;=');
	$reponse = req_mysql('S', 'contenu_scat', false, $where, $where_arg);
	while($donnees = lire_data_mysql($reponse))
	{
		$contenu = array();
		$reponse_bd = champs_table_bd('contenu');
		while($donnees_bd = lire_data_mysql($reponse_bd))
		{
			$contenu[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);
		}

		$data[] = $contenu;

	}

	return $data;
}

function contenu_produits($id_cat, $id_scat)
{
	global $prefix_session;

	$data = array();
	$where = array('Langue' => $_SESSION[$prefix_session . '_langue'], 'Type' => 'produits');
	$where_arg = array('=', 'AND;=');

	if($id_cat != 0)
	{
		$where['Categorie'] = $id_cat;
		$where_arg[] = 'AND;=';
	}
	if($id_scat != 0)
	{
		$where['Scat'] = $id_scat;
		$where_arg[] = 'AND;=';
	}

	$reponse = req_mysql('S', 'contenu', false, $where, $where_arg, 'ORDER BY Position, Titre');
	while($donnees = lire_data_mysql($reponse))
	{
		$contenu = array();
		$reponse_bd = champs_table_bd('contenu');
		while($donnees_bd = lire_data_mysql($reponse_bd))
		{
			$contenu[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);
		}

		$where = array('ID' => $contenu['Categorie'], 'Langue' => $_SESSION[$prefix_session . '_langue']);
		$where_arg = array('=', 'AND;=');
		$reponse_c = req_mysql('S', 'contenu', false, $where, $where_arg);
		$donnees_c = lire_data_mysql($reponse_c);

		$contenu['Titre_cat'] = ($donnees_c['Titre']);

		$where = array('ID' => $contenu['Scat'], 'Langue' => $_SESSION[$prefix_session . '_langue']);
		$where_arg = array('=', 'AND;=');
		$reponse_c = req_mysql('S', 'contenu_scat', false, $where, $where_arg);
		if($donnees_c = lire_data_mysql($reponse_c))
			$contenu['Titre_scat'] = ($donnees_c['Titre']);



		$data[] = $contenu;

	}
	return $data;
}

function contenu_cat_produits_url($url)
{
	global $prefix_session;

	$data = array();


	$where = array('Type' => 'produits_categorie', 'Langue' => $_SESSION[$prefix_session . '_langue']);
	$where_arg = array('=', 'AND;=');
	$reponse = req_mysql('S', 'contenu', false, $where, $where_arg);
	while($donnees = lire_data_mysql($reponse))
	{
		//echo creer_url(($donnees['Titre'])) . ' = ' . $url . '<br/>';
		if(creer_url(($donnees['Titre'])) == $url)
			return $donnees['ID'];

	}

	return 0;
}

function contenu_scat_produits_url($id_cat, $url)
{
	global $prefix_session;

	$data = array();


	$where = array('ID_C' => $id_cat, 'Langue' => $_SESSION[$prefix_session . '_langue']);
	$where_arg = array('=', 'AND;=');
	$reponse = req_mysql('S', 'contenu_scat', false, $where, $where_arg);
	while($donnees = lire_data_mysql($reponse))
	{
		//echo creer_url(($donnees['Titre'])) . ' = ' . $url . '<br/>';
		if(creer_url(($donnees['Titre'])) == $url)
			return $donnees['ID'];

	}

	return 0;
}

function contenu_produits_url($id_cat, $id_scat, $url)
{
	global $prefix_session;

	//echo 'Cat : ' . $id_cat . 'Scat : ' . $id_scat;
	$where = array('Categorie' => $id_cat, 'Scat' => $id_scat, 'Langue' => $_SESSION[$prefix_session . '_langue']);
	$where_arg = array('=', 'AND;=', 'AND;=');
	$reponse = req_mysql('S', 'contenu', false, $where, $where_arg);
	while($donnees = lire_data_mysql($reponse))
	{
		//echo 'produit-' . creer_url(($donnees['Titre'])) . ' = ' . $url . '<br/>';
		if('produit-' . creer_url(($donnees['Titre'])) == $url)
		{
			$contenu = array();
			$reponse_bd = champs_table_bd('contenu');
			while($donnees_bd = lire_data_mysql($reponse_bd))
			{
				$contenu[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);
			}
			return $contenu;
		}

	}

	return 0;
}


function contenu_partenaires()
{
    global $prefix_session;

    $data = array();

    $where = array('Type' => 'partenaires', 'Langue' => 'fr');
    $where_arg = array('=', 'AND;=');
    $reponse = req_mysql('S', 'contenu', false, $where, $where_arg);
    while($donnees = lire_data_mysql($reponse))
    {
        $contenu = array();
        $reponse_bd = champs_table_bd('contenu');
        while($donnees_bd = lire_data_mysql($reponse_bd))
        {
            $contenu[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);

            $where2 = array('ID_C' => $donnees['ID'], 'Lan' => $_SESSION[$prefix_session . '_langue']);
            $where_arg2 = array('=', 'AND;=');
            $reponse2 = req_mysql('S', 'contenu_cs', false, $where2, $where_arg2);
            $url = lire_data_mysql($reponse2);
            $contenu['Url'] = $url['Contenu'];
        }

        $data[] = $contenu;
    }

    // Filtrar los socios "ALH Marketing" y "Réseaux Web"
    $data = array_filter($data, function($partenaire) {
        return $partenaire['Titre'] !== 'ALH Marketing' && $partenaire['Titre'] !== 'Réseaux Web';
    });

    return $data;
}


function contenu_temoignages($lang)
{
	global $prefix_session;

	$data = array();


	$where = array('Type' => 'temoignages', 'Langue' => $lang);
	$where_arg = array('=', 'AND;=');
	$reponse = req_mysql('S', 'contenu', false, $where, $where_arg);
	while($donnees = lire_data_mysql($reponse))
	{
		if($donnees['Titre'] != '')
		{
			$contenu = array();
			$reponse_bd = champs_table_bd('contenu');
			while($donnees_bd = lire_data_mysql($reponse_bd))
			{
				$contenu[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);

				$where2 = array('ID_C' => $donnees['ID'], 'Lan' => $lang);
				$where_arg2 = array('=', 'AND;=');
				$reponse2 = req_mysql('S', 'contenu_cs', false, $where2, $where_arg2);
				$url = lire_data_mysql($reponse2);
				$contenu['Nom_complet'] = ($url['Contenu']);
			}

			$data[] = $contenu;
		}

	}

	return $data;
}

function contenu_realisations($lang, $nb = false, $home = false)
{
	global $prefix_session;

	$data = array();

	/*if($nb === false)
		$limit = false;
	else
		$limit = " LIMIT 0, " . $nb;*/
	$where = array('Type' => 'realisations', 'Langue' => $lang);
	$where_arg = array('=', 'AND;=');
	$reponse = req_mysql('S', 'contenu', false, $where, $where_arg, 'Order by ID DESC ', false);
	while($donnees = lire_data_mysql($reponse))
	{
		$contenu = array();
		$reponse_bd = champs_table_bd('contenu');
		while($donnees_bd = lire_data_mysql($reponse_bd))
		{
			$contenu[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);

			$where2 = array('ID' => $donnees['Categorie'], 'Langue' => $lang);
			$where_arg2 = array('=', 'AND;=');
			$reponse2 = req_mysql('S', 'contenu', false, $where2, $where_arg2);
			$categorie = lire_data_mysql($reponse2);
			$contenu['Nom_categorie'] = ($categorie['Description']);
		}

		$contenu['Description'] = str_replace('src="../', 'src="/',$contenu['Description']);

		$data_cs = contenu_cs($donnees['ID'], $donnees['Type']);
		/*echo '<pre>';
		print_r($data_cs);
		echo '</pre>';//*/
		if(isset($data_cs['CS_3']) AND $data_cs['CS_3'] == 1)
			$contenu['Home'] = 1;
		else
			$contenu['Home'] = 0;

        if(isset($data_cs['CS_5']) AND trim($data_cs['CS_5']) != '')
            $contenu['Preview'] =$data_cs['CS_5'];

		$data[] = $contenu;

	}

	if($home === false)
	{

		return $data;
	}
	else
	{
		//tri + sélection
		$array_final = array();
		$finaux = array();
		foreach($data AS $k => $v)
		{

			$ajout = false;
			if($nb !== false AND count($finaux) < $nb)
				$ajout = true;
			else
				$ajout = false;

			if($ajout)
			{

				if($v['Home'] == 1)
				{
					$finaux[] = $data[$k];
					/*echo 'Ajout';
					print_r($data[$k]);*/
					$array_final = $k;
				}
			}
		}

		/*if($nb !== false )
		{
			for($i = 0; $i < $nb; $i++)
			{
				if(!in_array($i, $array_final ))
				{
					$finaux[] = $data[$i];
					$array_final = $i;
				}
			}
		}	*/
		return $finaux;
	}



}
function contenu_realisations_rand($lang)
{
	global $prefix_session;

	$data = array();


	$where = array('Type' => 'realisations', 'Langue' => $lang);
	$where_arg = array('=', 'AND;=');
	$reponse = req_mysql('S', 'contenu', false, $where, $where_arg, 'Order by rand() LIMIT 12', false);
	while($donnees = lire_data_mysql($reponse))
	{
		$contenu = array();
		$reponse_bd = champs_table_bd('contenu');
		while($donnees_bd = lire_data_mysql($reponse_bd))
		{
			$contenu[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);

			$where2 = array('ID' => $donnees['Categorie'], 'Langue' => $lang);
			$where_arg2 = array('=', 'AND;=');
			$reponse2 = req_mysql('S', 'contenu', false, $where2, $where_arg2);
			$categorie = lire_data_mysql($reponse2);
			$contenu['Nom_categorie'] = ($categorie['Description']);

		}

		$contenu['Description'] = str_replace('src="../', 'src="/',$contenu['Description']);

		$data[] = $contenu;

	}

	return $data;
}


function liste_contenu($type, $langue, $nb = false)
{
	$array = array();

	if($nb == false)
		$limit = false;
	else
		$limit = "LIMIT 0, " . $nb;

	$where = array('Type' => $type, 'Langue' => $langue, 'Visible' => 1 );
	$where_arg = array('=', 'AND;=', 'AND;=');
	$reponse = req_mysql('S', 'contenu', false, $where, $where_arg, 'ORDER BY Position, ID DESC', $limit);
	while($donnees = lire_data_mysql($reponse))
	{
		$contenu = array();
		$reponse_bd = champs_table_bd('contenu');
		while($donnees_bd = lire_data_mysql($reponse_bd))
		{
			$contenu[$donnees_bd['Field']] = ($donnees[$donnees_bd['Field']]);
		}
		$data_cs = array();
		$data_cs = contenu_cs($donnees['ID'], $donnees['Type']);
		foreach($data_cs AS $k =>$v)
		{
			$contenu[$k] = $v;
			//echo $v;
		}
		$array[] = $contenu;
	}
	/*echo '<pre>';
	print_r($array);
	echo '</pre>';//*/

	return $array;

}