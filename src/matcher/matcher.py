from database.mongo import resume_collection, job_collection

PRIMARY_WEIGHT = 50
SECONDARY_WEIGHT = 20
EXPERIENCE_WEIGHT = 15
LOCATION_WEIGHT = 5
EDUCATION_WEIGHT = 10


def normalize_list(value):
    if isinstance(value, list):
        return [str(v).strip().lower() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [v.strip().lower() for v in value.split(",") if v.strip()]
    return []


def match_resume_to_jobs(candidate_id: str, top_n: int = 5):

    # -------------------------
    # Fetch Resume
    # -------------------------
    resume = resume_collection.find_one({"candidate_id": candidate_id})
    if not resume:
        print("Resume not found")
        return []

    # Normalize resume fields
    resume_primary = normalize_list(resume.get("primary_skills"))
    resume_secondary = normalize_list(resume.get("secondary_skills"))
    resume_experience = resume.get("experience_years") or 0
    resume_location = str(resume.get("location") or "").strip().lower()
    resume_education = normalize_list(resume.get("education"))

    # -------------------------
    # Aggregation Pipeline
    # -------------------------
    pipeline = [

        # Step 1: Basic filtering
        {
            "$match": {
                "$or": [
                    {"primary_skills": {"$exists": True}},
                    {"secondary_skills": {"$exists": True}}
                ]
            }
        },

        # Step 2: Normalize job fields safely
        {
            "$addFields": {

                "primary_array": {
                    "$cond": {
                        "if": {"$isArray": "$primary_skills"},
                        "then": {
                            "$map": {
                                "input": "$primary_skills",
                                "as": "skill",
                                "in": {"$toLower": "$$skill"}
                            }
                        },
                        "else": {
                            "$cond": {
                                "if": {"$eq": [{"$type": "$primary_skills"}, "string"]},
                                "then": {
                                    "$map": {
                                        "input": {"$split": ["$primary_skills", ","]},
                                        "as": "skill",
                                        "in": {"$toLower": {"$trim": {"input": "$$skill"}}}
                                    }
                                },
                                "else": []
                            }
                        }
                    }
                },

                "secondary_array": {
                    "$cond": {
                        "if": {"$isArray": "$secondary_skills"},
                        "then": {
                            "$map": {
                                "input": "$secondary_skills",
                                "as": "skill",
                                "in": {"$toLower": "$$skill"}
                            }
                        },
                        "else": {
                            "$cond": {
                                "if": {"$eq": [{"$type": "$secondary_skills"}, "string"]},
                                "then": {
                                    "$map": {
                                        "input": {"$split": ["$secondary_skills", ","]},
                                        "as": "skill",
                                        "in": {"$toLower": {"$trim": {"input": "$$skill"}}}
                                    }
                                },
                                "else": []
                            }
                        }
                    }
                },

                "education_array": {
                    "$cond": {
                        "if": {"$isArray": "$education"},
                        "then": {
                            "$map": {
                                "input": "$education",
                                "as": "edu",
                                "in": {"$toLower": "$$edu"}
                            }
                        },
                        "else": []
                    }
                },

                "job_location_lower": {
                    "$toLower": {
                        "$cond": {
                            "if": {"$eq": [{"$type": "$location"}, "string"]},
                            "then": "$location",
                            "else": ""
                        }
                    }
                }
            }
        },

        # Step 3: Skill intersection
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

        # Step 4: Weighted scoring
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
                        {"$eq": ["$job_location_lower", resume_location]},
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
                                            "$education_array",
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

        # Step 5: Total Score
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

        # Step 6: Sort and limit
        {"$sort": {"total_score": -1}},
        {"$limit": top_n},

        # Step 7: Output
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

    return list(job_collection.aggregate(pipeline))