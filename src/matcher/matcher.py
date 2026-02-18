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

    pipeline = [
        {
            "$addFields": {
                "primary_match": {
                    "$size": {
                        "$setIntersection": ["$primary_skills", resume["primary_skills"]]
                    }
                },
                "secondary_match": {
                    "$size": {
                        "$setIntersection": ["$secondary_skills", resume["secondary_skills"]]
                    }
                }
            }
        },
        {
            "$addFields": {
                "primary_score": {
                    "$multiply": [
                        {"$divide": ["$primary_match", {"$max": [{"$size": "$primary_skills"}, 1]}]},
                        PRIMARY_WEIGHT
                    ]
                },
                "secondary_score": {
                    "$multiply": [
                        {"$divide": ["$secondary_match", {"$max": [{"$size": "$secondary_skills"}, 1]}]},
                        SECONDARY_WEIGHT
                    ]
                }
            }
        },
        {
            "$addFields": {
                "experience_score": {
                    "$cond": [
                        {"$gte": [resume["experience_years"], "$min_experience"]},
                        EXPERIENCE_WEIGHT,
                        0
                    ]
                }
            }
        },
        {
            "$addFields": {
                "total_score": {
                    "$round": [
                        {"$add": ["$primary_score", "$secondary_score", "$experience_score"]},
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
                "primary_match": 1,
                "secondary_match": 1,
                "total_score": 1
            }
        }
    ]

    

    return list(job_collection.aggregate(pipeline))


