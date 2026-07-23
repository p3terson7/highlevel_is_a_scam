from app.services.lead_summary import filter_question_form_answers, normalize_form_answers


def test_normalize_form_answers_folds_accents_in_french_question_keys():
    answers = normalize_form_answers(
        {
            "Secteur d'activité": "Entreprise",
            "Délai de réalisation souhaité?": "Dans les 5 jours ouvrables",
            "Sélectionner les services requis": "Scan 3D",
        }
    )

    assert answers == {
        "secteur_d_activite": "Entreprise",
        "delai_de_realisation_souhaite": "Dans les 5 jours ouvrables",
        "selectionner_les_services_requis": "Scan 3D",
    }


def test_normalize_form_answers_preserves_english_aliases_and_key_formatting():
    answers = normalize_form_answers(
        {
            "Timeline": "Within two weeks",
            "Mobile Phone": "+1 416 555 0100",
            "Main Challenge": "Legacy drawings",
        }
    )

    assert answers == {
        "when_to_start": "Within two weeks",
        "phone_number": "+1 416 555 0100",
        "biggest_marketing_challenge": "Legacy drawings",
    }


def test_filter_question_form_answers_excludes_internal_form_routing_metadata():
    answers = filter_question_form_answers(
        {
            "Form Type": "quote_request",
            "Lang": "fr",
            "Type Client": "Individual",
            "Services": "Scan 3D",
        }
    )

    assert answers == {
        "type_client": "Individual",
        "services": "Scan 3D",
    }
