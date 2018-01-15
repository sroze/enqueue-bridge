<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Sam\Symfony\Bridge\EnqueueMessage;

use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpQueue;
use Interop\Amqp\AmqpTopic;
use Symfony\Component\Message\Transport\Serialization\EncoderInterface;

/**
 * Symfony Message sender to bridge Php-Enqueue producers.
 *
 * @author Samuel Roze <samuel.roze@gmail.com>
 */
class AmqpInteropSender extends QueueInteropSender
{
    /**
     * @var string
     */
    private $destinationName;

    /**
     * @var bool
     */
    private $isTopic;

    /**
     * @var AmqpContext
     */
    private $context;

    public function __construct(
        EncoderInterface $messageEncoder,
        AmqpContext $context,
        string $destinationName,
        bool $isTopic = true,
        float $deliveryDelay = null,
        float $timeToLive = null,
        int $priority = null
    ) {
        parent::__construct(
            $messageEncoder,
            $context,
            $destinationName,
            $isTopic,
            $deliveryDelay,
            $timeToLive,
            $priority
        );

        $this->destinationName = $destinationName;
        $this->isTopic = $isTopic;
    }

    /**
     * {@inheritdoc}
     */
    public function send($message)
    {
        if ($this->isTopic) {
            $destination = $this->context->createTopic($this->destinationName);
            $destination->setType(AmqpTopic::TYPE_FANOUT);
            $destination->addFlag(AmqpTopic::FLAG_DURABLE);

            $this->context->declareTopic($destination);
        } else {
            $destination = $this->context->createQueue($this->destinationName);
            $destination->addFlag(AmqpQueue::FLAG_DURABLE);

            $this->context->declareQueue($destination);
        }

        parent::send($message);
    }
}
