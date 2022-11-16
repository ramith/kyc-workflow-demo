import ballerina/http;


type Customer record {
    string accountId;
    string firstName;
    string lastName;
};



# A service representing a network-accessible API
# bound to port `9090`.
service / on new http:Listener(9090) {

    resource function post validate(@http:Payload Customer[] customers) returns error? {

    }
}
