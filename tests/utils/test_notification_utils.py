import unittest

from data_juicer.utils.notification_utils import send_test_card_notification


class TestCardNotificationTest(unittest.TestCase):

    def test_sends_test_card_without_cookie_header(self):
        result = send_test_card_notification(
            template_id="AAqt1lQ72dVxK",
            template_variable={"input": {"k1": "v1", "k2": "v2"}},
            user_email_or_account="wangjianda.667@bytedance.com",
        )

        self.assertEqual(result["ok"], True)
        self.assertEqual(result["status_code"], 200)
        self.assertIsInstance(result["data"], dict)


if __name__ == "__main__":
    unittest.main()
