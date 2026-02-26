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


def safe_array(field_name):
    return {
        "$cond": {
            "if": {"$isArray": f"${field_name}"},
            "then": {
                "$map": {
                    "input": f"${field_name}",
                    "as": "item",
                    "in": {
                        "$cond": [
                            {"$eq": [{"$type": "$$item"}, "string"]},
                            {"$toLower": "$$item"},
                            ""
                        ]
                    }
                }
            },
            "else": {
                "$cond": {
                    "if": {"$eq": [{"$type": f"${field_name}"}, "string"]},
                    "then": [{"$toLower": f"${field_name}"}],
                    "else": []
                }
            }
        }
    }

def match_resume_to_jobs(candidate_id: str, top_n: int = 5):
    resume = resume_collection.find_one({"candidate_id": candidate_id})
    if not resume:
        return []

    resume_primary = normalize_list(resume.get("primary_skills"))
    resume_secondary = normalize_list(resume.get("secondary_skills"))
    resume_location = normalize_list(resume.get("location"))
    resume_education = normalize_list(resume.get("education"))
    resume_experience = resume.get("experience_years", 0)

    pipeline = [
    # 🔹 Pre-filter jobs using $match
    {
        "$match": {
            "$or": [
                {"primary_skills": {"$in": resume_primary}},
                {"secondary_skills": {"$in": resume_secondary}}
            ]
        }
    },

    # 🔹 Normalize fields safely
    {
        "$addFields": {
            "primary_array": safe_array("primary_skills"),
            "secondary_array": safe_array("secondary_skills"),
            "education_array": safe_array("education"),
            "location_array": safe_array("location")
        }
    },

    # 🔹 Compute skill intersections
    {
        "$addFields": {
            "primary_match": {"$size": {"$setIntersection": ["$primary_array", resume_primary]}},
            "secondary_match": {"$size": {"$setIntersection": ["$secondary_array", resume_secondary]}}
        }
    },

    # 🔹 Compute scores
    {
        "$addFields": {
            "primary_score": {
                "$multiply": [
                    {"$divide": ["$primary_match", {"$max": [{"$size": "$primary_array"}, 1]}]},
                    PRIMARY_WEIGHT
                ]
            },
            "secondary_score": {
                "$multiply": [
                    {"$divide": ["$secondary_match", {"$max": [{"$size": "$secondary_array"}, 1]}]},
                    SECONDARY_WEIGHT
                ]
            },
            "experience_score": {
                "$cond": [
                    {"$gte": ["$experience_years", resume_experience]},
                    EXPERIENCE_WEIGHT,
                    0
                ]
            },
            "location_score": {
                "$cond": [
                    {"$gt": [{"$size": {"$setIntersection": ["$location_array", resume_location]}}, 0]},
                    LOCATION_WEIGHT,
                    0
                ]
            },
            "education_score": {
                "$cond": [
                    {"$gt": [{"$size": {"$setIntersection": ["$education_array", resume_education]}}, 0]},
                    EDUCATION_WEIGHT,
                    0
                ]
            }
        }
    },

    # 🔹 Total score
    {
        "$addFields": {
            "total_score": {
                "$round": [
                    {"$add": ["$primary_score", "$secondary_score", "$experience_score", "$location_score", "$education_score"]},
                    2
                ]
            }
        }
    },

    # 🔹 Sort and limit
    {"$sort": {"total_score": -1}},
    {"$limit": top_n},

    # 🔹 Final projection
    {
        "$project": {
            "_id": 0,
            "job_id": 1,
            "job_summary": 1,
            "technology": 1,
            "category": 1,
            "total_score": 1
        }
    }
]

    return list(job_collection.aggregate(pipeline))


def match_job_to_resumes(job_id: str, top_n: int = 5):
    job = job_collection.find_one({"job_id": job_id})
    if not job:
        return []

    job_primary = normalize_list(job.get("primary_skills"))
    job_secondary = normalize_list(job.get("secondary_skills"))
    job_location = normalize_list(job.get("location"))
    job_education = normalize_list(job.get("education"))
    job_experience = job.get("minimum_experience_in_years", 0)

    pipeline = [
        {
            "$match": {
                "$or": [
                    {"primary_skills": {"$in": job_primary}},
                    {"secondary_skills": {"$in": job_secondary}}
                ]
            }
        },

        {
            "$addFields": {
                "primary_array": safe_array("primary_skills"),
                "secondary_array": safe_array("secondary_skills"),
                "education_array": safe_array("education"),
                "location_array": safe_array("location"),
            }
        },

        {
            "$addFields": {
                "primary_match": {"$size": {"$setIntersection": ["$primary_array", job_primary]}},
                "secondary_match": {"$size": {"$setIntersection": ["$secondary_array", job_secondary]}}
            }
        },

        {
            "$addFields": {
                "primary_score": {
                    "$multiply": [
                        {"$divide": ["$primary_match", {"$max": [{"$size": "$primary_array"}, 1]}]},
                          PRIMARY_WEIGHT
                    ]
                },
                "secondary_score": {
                    "$multiply": [
                        {"$divide": ["$secondary_match", {"$max": [{"$size": "$secondary_array"}, 1]}]}, 
                        SECONDARY_WEIGHT
                    ]
                },
                "experience_score": {
                    "$cond": [
                        {"$gte": ["$experience_years", job_experience]}, EXPERIENCE_WEIGHT
                        , 0
                    ]
                },
                "location_score": {
                    "$cond": [
                        {"$gt": [{"$size": {"$setIntersection": ["$location_array", job_location]}}, 0]}, 
                        LOCATION_WEIGHT, 
                        0
                    ]
                },
                "education_score": {
                    "$cond": [
                        {"$gt": [{"$size": {"$setIntersection": ["$education_array", job_education]}}, 0]},
                        EDUCATION_WEIGHT, 
                        0
                    ]
                },
        }},
        {"$addFields": {
            "total_score": {"$round": [{"$add": ["$primary_score", "$secondary_score", "$experience_score", "$location_score", "$education_score"]}, 2]}
        }},
        {"$sort": {"total_score": -1}},
        {"$limit": top_n},
        {"$project": {"_id": 0, "candidate_id": 1, "name": 1, "email": 1, "total_score": 1}}
    ]

    return list(resume_collection.aggregate(pipeline))