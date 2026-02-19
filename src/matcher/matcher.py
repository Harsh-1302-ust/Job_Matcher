from database.mongo import resume_collection, job_collection

PRIMARY_WEIGHT = 50
SECONDARY_WEIGHT = 20
EXPERIENCE_WEIGHT = 15
LOCATION_WEIGHT = 5
EDUCATION_WEIGHT = 10


def match_resume_to_jobs(candidate_id: str, top_n: int = 5):

    resume = resume_collection.find_one({"candidate_id": candidate_id})
    if not resume:
        print("Resume not found")
        return []

    # Force resume fields to arrays safely
    resume_primary = resume.get("primary_skills") or []
    resume_secondary = resume.get("secondary_skills") or []
    resume_experience = resume.get("experience_years") or 0
    resume_location = (resume.get("location") or "").lower()
    resume_education = resume.get("education") or []

    if isinstance(resume_primary, str):
        resume_primary = [resume_primary]

    if isinstance(resume_secondary, str):
        resume_secondary = [resume_secondary]

    if isinstance(resume_education, str):
        resume_education = [resume_education]

    pipeline = [

        {
            "$match": {
                "$or": [
                    {"primary_skills": {"$in": resume_primary}},
                    {"secondary_skills": {"$in": resume_secondary}}
                ]
            }
        },

        # SAFE ARRAY CONVERSION INSIDE PIPELINE
        {
            "$addFields": {

                "safe_primary_skills": {
                    "$cond": [
                        {"$isArray": "$primary_skills"},
                        "$primary_skills",
                        {
                            "$cond": [
                                {"$ne": ["$primary_skills", None]},
                                ["$primary_skills"],
                                []
                            ]
                        }
                    ]
                },

                "safe_secondary_skills": {
                    "$cond": [
                        {"$isArray": "$secondary_skills"},
                        "$secondary_skills",
                        {
                            "$cond": [
                                {"$ne": ["$secondary_skills", None]},
                                ["$secondary_skills"],
                                []
                            ]
                        }
                    ]
                },

                "safe_education_required": {
                    "$cond": [
                        {"$isArray": "$education_required"},
                        "$education_required",
                        {
                            "$cond": [
                                {"$ne": ["$education_required", None]},
                                ["$education_required"],
                                []
                            ]
                        }
                    ]
                }
            }
        },

        # SKILL MATCH
        {
            "$addFields": {
                "primary_match": {
                    "$size": {
                        "$setIntersection": [
                            "$safe_primary_skills",
                            resume_primary
                        ]
                    }
                },
                "secondary_match": {
                    "$size": {
                        "$setIntersection": [
                            "$safe_secondary_skills",
                            resume_secondary
                        ]
                    }
                }
            }
        },

        # SCORING
        {
            "$addFields": {

                "primary_score": {
                    "$multiply": [
                        {
                            "$divide": [
                                "$primary_match",
                                {"$max": [{"$size": "$safe_primary_skills"}, 1]}
                            ]
                        },
                        PRIMARY_WEIGHT
                    ]
                },

                "secondary_score": {
                    "$multiply": [
                        {
                            "$divide": [
                                "$secondary_match",
                                {"$max": [{"$size": "$safe_secondary_skills"}, 1]}
                            ]
                        },
                        SECONDARY_WEIGHT
                    ]
                },

                "experience_score": {
                    "$cond": [
                        {"$gte": [resume_experience, {"$ifNull": ["$min_experience", 0]}]},
                        EXPERIENCE_WEIGHT,
                        0
                    ]
                },

                "location_score": {
                    "$cond": [
                        {
                            "$eq": [
                                {"$toLower": {"$ifNull": ["$location", ""]}},
                                resume_location
                            ]
                        },
                        LOCATION_WEIGHT,
                        0
                    ]
                },

                "education_score": {
                    "$cond": [
                        {
                            "$gt": [
                                {
                                    "$size": {
                                        "$setIntersection": [
                                            "$safe_education_required",
                                            resume_education
                                        ]
                                    }
                                },
                                0
                            ]
                        },
                        EDUCATION_WEIGHT,
                        0
                    ]
                }
            }
        },

        {
            "$addFields": {
                "total_score": {
                    "$round": [
                        {
                            "$add": [
                                "$primary_score",
                                "$secondary_score",
                                "$experience_score",
                                "$location_score",
                                "$education_score"
                            ]
                        },
                        2
                    ]
                }
            }
        },

        {"$sort": {"total_score": -1}},
        {"$limit": top_n},

        {
            "$project": {
                "_id": 0,
                "job_id": 1,
                "job_title": 1,
                "total_score": 1
            }
        }
    ]

    return list(job_collection.aggregate(pipeline))
