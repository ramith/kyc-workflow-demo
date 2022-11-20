import ramith/customer_analytics;
import ballerina/http;
import ramith/maps_api;
import ballerina/log;
import ramith/kyc_api;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;
import ballerina/time;
import ballerina/sql;
import ballerina/regex;

configurable string clientSecret = ?;
configurable string clientId = ?;

configurable string dbHost = ?;
configurable string dbUser = ?;
configurable string dbPassword = ?;
configurable string dbName = ?;
configurable int dbPort = ?;

public type Customer record {
    @sql:Column { name: "account_id" }
    string accountId;
    @sql:Column { name: "first_name" }
    string firstName;
    @sql:Column { name: "last_name" }
    string lastName;
    maps_api:Address address;
};

public type VerificationResult record {
    string accountId;
    string message;
};

mysql:Client mysqlEp = check new (
    host = dbHost,
    user = dbUser,
    password = dbPassword,
    database = dbName,
    port = dbPort
);

# A service representing a network-accessible API
# bound to port `9090`.
service / on new http:Listener(9090) {

    resource function post validate(@http:Payload Customer[] customers) returns VerificationResult[]|error {

        VerificationResult[] verificationResults = [];

        map<future<error?>> childProcesses = {};

        foreach Customer c in customers {
            log:printInfo("validating customer:", accountId = c.accountId, name = string `${c.firstName} ${c.lastName}`);
            boolean validAddress = check validateAddress(c.address);
            if !validAddress {
                verificationResults.push({
                    accountId: c.accountId,
                    message: "address is invalid"
                });

                continue;
            }

            boolean validKyc = check validateKyc(c.accountId);
            if !validKyc {
                _ = check addToKycReprocessingList(c.accountId);
                verificationResults.push({
                    accountId: c.accountId,
                    message: "invalid kyc data, added to reprocessing list"
                });

                continue;
            }

            // send the customer info systems of record.
            future<error?> asyncResult = start sendToCustomerAnalytics(c.accountId);
            childProcesses[c.accountId] = asyncResult;
        }

        foreach [string, future<error?>] entry in childProcesses.entries() {
            var [accountId, outcome] = entry;
            error? err = wait outcome;
            if err is () {

                log:printInfo("sending customer information to system of records was successful", accountId = accountId);
                verificationResults.push({
                    accountId: accountId,
                    message: "successfully verified, sent to customer analytics"
                });
            } else {
                verificationResults.push({
                    accountId: accountId,
                    message: "customer verification unsuccessful"
                });
                log:printError("error occurred while sending information to system of records", err);
            }
        }

        return verificationResults;
    }

}

function validateAddress(maps_api:Address address) returns boolean|error {

    maps_api:Client maps_apiEp = check new (clientConfig = {
        auth: {
            clientId: clientId,
            clientSecret: clientSecret
        }
    });

    maps_api:Address|error addressValidationResponse = maps_apiEp->postMapsAddressValidate(payload = address);
    if addressValidationResponse is error {
        log:printError("unable to find the address", address = address);
        return false;
    }
    return true;
}

function validateKyc(string accountId) returns boolean|error {
    kyc_api:Client kycApiEp = check new (clientConfig = {
        auth: {
            clientId: clientId,
            clientSecret: clientSecret
        }
    });

    kyc_api:KycInfo kycValidationResponse = check kycApiEp->postMapsKycAccountid(accountId);
    if kycValidationResponse.state != "verified" {
        log:printError("kyc validation is failed", accountId = accountId, state = kycValidationResponse.state);
        return false;
    }

    return true;
}

function addToKycReprocessingList(string accountId) returns error? {
    sql:ExecutionResult _ = check mysqlEp->execute(`INSERT INTO reprocess_kyc (account_Id) values (${accountId})`);
    log:printInfo("successfully added to kyc reprocessing list", accountId = accountId);
}

function sendToCustomerAnalytics(string accountId) returns error? {
    log:printInfo("sending to system of records", accNo = accountId);
    check validateCustomer(accountId);
    customer_analytics:Client customer_analyticsEp = check new (clientConfig = {
        auth: {
            clientId: clientId,
            clientSecret: clientSecret
        }
    });
    http:Response _ = check customer_analyticsEp->postCustomerVerification(payload = {
        accountId: accountId,
        status: "verified",
        description: "successfully verified",
        verifiedOn: time:utcToString(time:utcNow())
    });
}

function validateCustomer(string accountId) returns error? {
    Customer customer = check mysqlEp->queryRow(
        sqlQuery = `SELECT * from customer WHERE account_id=${accountId}`
    );
    if (!regex:matches(customer.firstName, "[A-Za-z ]+") || !regex:matches(customer.lastName, "[A-Za-z ]+")) {
        return error(string `invalid customer data for id ${accountId}`);
    }
}
