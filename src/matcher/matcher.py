from database.mongo import resume_collection, job_collection

PRIMARY_WEIGHT = 50
SECONDARY_WEIGHT = 20
EXPERIENCE_WEIGHT = 15
LOCATION_WEIGHT = 5
EDUCATION_WEIGHT = 10


def match_resume_to_jobs(candidate_id: str, top_n: int = 5):

    # Fetch Resume
    resume = resume_collection.find_one({"candidate_id": candidate_id})
    if not resume:
        print("Resume not found")
        return []

    # Extract resume safely
    resume_primary = resume.get("primary_skills", [])
    resume_secondary = resume.get("secondary_skills", [])
    resume_experience = resume.get("experience_years", 0)
    resume_location = (resume.get("location") or "").lower()
    resume_education = resume.get("education", [])

    # Normalize resume skills if stored as string
    if isinstance(resume_primary, str):
        resume_primary = [s.strip().lower() for s in resume_primary.split(",") if s.strip()]

    if isinstance(resume_secondary, str):
        resume_secondary = [s.strip().lower() for s in resume_secondary.split(",") if s.strip()]

    if not isinstance(resume_primary, list):
        resume_primary = []

    if not isinstance(resume_secondary, list):
        resume_secondary = []

    if not isinstance(resume_education, list):
        resume_education = []

    pipeline = [

        # Step 1: Basic skill filtering
        {
            "$match": {
                "$or": [
                    {"primary_skills": {"$exists": True}},
                    {"secondary_skills": {"$exists": True}}
                ]
            }
        },

        # Step 2: Safe skill intersection
        {
            "$addFields": {

                "primary_array": {
                    "$cond": [
                        {"$isArray": "$primary_skills"},
                        "$primary_skills",
                        {
                            "$split": [
                                {"$ifNull": ["$primary_skills", ""]},
                                ","
                            ]
                        }
                    ]
                },

                "secondary_array": {
                    "$cond": [
                        {"$isArray": "$secondary_skills"},
                        "$secondary_skills",
                        {
                            "$split": [
                                {"$ifNull": ["$secondary_skills", ""]},
                                ","
                            ]
                        }
                    ]
                }
            }
        },

        {
            "$addFields": {

                "primary_match": {
                    "$size": {
                        "$setIntersection": [
                            "$primary_array",
                            resume_primary
                        ]
                    }
                },

                "secondary_match": {
                    "$size": {
                        "$setIntersection": [
                            "$secondary_array",
                            resume_secondary
                        ]
                    }
                }
            }
        },

        # Step 3: Scoring
        {
            "$addFields": {

                "primary_score": {
                    "$multiply": [
                        {
                            "$divide": [
                                "$primary_match",
                                {"$max": [{"$size": "$primary_array"}, 1]}
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
                                {"$max": [{"$size": "$secondary_array"}, 1]}
                            ]
                        },
                        SECONDARY_WEIGHT
                    ]
                },

                "experience_score": {
                    "$cond": [
                        {
                            "$gte": [
                                resume_experience,
                                {"$ifNull": ["$minimum_experience_in_years", 0]}
                            ]
                        },
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
                                            {"$ifNull": ["$education", []]},
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

        # Step 4: Total Score
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

        # Step 5: Sort and limit
        {"$sort": {"total_score": -1}},
        {"$limit": top_n},

        # Step 6: Output
        {
            "$project": {
                "_id": 0,
                "job_id": 1,
                "job_summary": 1,
                "technology": 1,
                "category": 1,
                "total_score": 1,
                "primary_score": 1,
                "secondary_score": 1,
                "experience_score": 1,
                "location_score": 1,
                "education_score": 1
            }
        }
    ]

    results = list(job_collection.aggregate(pipeline))
    return results